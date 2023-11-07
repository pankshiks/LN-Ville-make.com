from fastapi import FastAPI, UploadFile, HTTPException
from app.processor import DataProcessor
from app.generate_pdf import InvoiceProcessor
from pydantic import BaseModel
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from typing import List
import os
import openpyxl
import pandas as pd
import requests
import base64
import json


app = FastAPI()
base_url = os.environ.get("BASE_URL", "http://127.0.0.1:8000")

# Define the directory path
directory_path = "final_folder"
one_more = "invoice_folder"
csv_data_directory = "app/data"
csv_file_name = "clients_and_projects.csv"


# Check if the directory exists, and create it if it doesn't
if not os.path.exists(directory_path):
    os.makedirs(directory_path)

# Check if the directory exists, and create it if it doesn't
if not os.path.exists(one_more):
    os.makedirs(one_more)

# Mount the directory as a static directory
app.mount("/pdfs", StaticFiles(directory=directory_path), name="pdfs")

pdf_folder = "final_folder"  # This is the folder where your PDFs are stored


def is_valid_file_extension(filename, valid_extensions):
    ext = filename.split(".")[-1]
    return ext in valid_extensions


# Define the send_data_to_webhook function to send data to the webhook
def send_data_to_webhook(data: dict, files: dict):
    webhook_url = "https://hook.eu2.make.com/e5ql7jh487prsm5551jegt98wr2l463p"
    try:
        files_data = [
            ("file_data", (file_name, file_data, content_type))
            for file_name, file_data, content_type in files["files"]
        ]
        response = requests.post(
            webhook_url, data={"fulldata": json.dumps(data)}, files=files_data
        )
        response.raise_for_status()  # Raise an exception for HTTP errors

        return {"message": "Data sent to the webhook successfully"}

    except requests.exceptions.RequestException as e:
        return {"message": f"Failed to send data to the webhook: {str(e)}"}


def calculate_amount_sum(csv_folder_path, pdf_folder_path):
    try:
        # Create an empty list to store the results
        results = []
        # Create a separate list for files
        files_list = []

        # Iterate through all files in the folder
        for file_name in os.listdir(csv_folder_path):
            # Construct the full path to the CSV file
            file_path = os.path.join(csv_folder_path, file_name)
            company_name = file_name.split("-")
            # Read the CSV file into a Pandas DataFrame
            df = pd.read_csv(file_path)
            csv_file_path = os.path.join(csv_data_directory, csv_file_name)
            print(csv_file_path)
            clients_df = pd.read_csv(csv_file_path)
            filtered_df = clients_df[
                clients_df["Project"].str.contains(
                    company_name[0], case=False, na=False
                )
            ]

            if not filtered_df.empty:
                filtered_data = filtered_df.iloc[0].to_dict()
                total_amount = df["Amount"].sum()

                # Create a result dictionary and append it to the list
                result = {
                    "total_amount": total_amount,
                    "filtered_data": filtered_data,
                }
                results.append(result)

                # Get the corresponding PDF file name
                pdf_file_name = os.path.splitext(file_name)[0] + ".pdf"
                pdf_file_path = os.path.join(pdf_folder_path, pdf_file_name)

                # Read the PDF file in binary format
                with open(pdf_file_path, "rb") as pdf_file:
                    pdf_binary = pdf_file.read()

                # Create a files dictionary for the PDF file
                files = (pdf_file_name, pdf_binary, "application/pdf")
                files_list.append(files)

        return results, files_list
    except Exception as e:
        print("Not Working", e)
        return None


class ProcessInvoicesResponse(BaseModel):
    message: str
    pdf_urls: List[str]


@app.post("/process_data_and_invoices", response_model=ProcessInvoicesResponse)
async def process_data_and_invoices(
    pay_journal: UploadFile,
    daily_cost_detail: UploadFile,
    input_charge_Sheet: UploadFile,
    job_classification: UploadFile,
):
    valid_extensions = ["csv", "xlsx"]
    files = [pay_journal, daily_cost_detail, input_charge_Sheet, job_classification]
    file_names = [
        "Pay Journal (CSV).csv",
        "Daily Cost Detail - Actual (CSV).csv",
        "Charge Sheet.csv",
        "Job_Classifications.csv",
    ]

    # Check file extensions for validity
    for file in files:
        if not is_valid_file_extension(file.filename, valid_extensions):
            raise HTTPException(
                status_code=400,
                detail="Invalid file extension. Supported extensions are .csv and .xlsx.",
            )

    # Define the directory where the files will be saved
    upload_dir = "uploads"
    os.makedirs(upload_dir, exist_ok=True)

    for i, file in enumerate(files):
        content = await file.read()
        filename = file_names[i]
        with open(os.path.join(upload_dir, filename), "wb") as f:
            f.write(content)

    # Create a new workbook for the combined XLSX file
    combined_file_path = os.path.join(
        upload_dir, "CYP invoice query FY 24 Auto Reconciliation.xlsm"
    )
    combined_wb = openpyxl.Workbook()

    for source_filename in ["Job_Classifications.csv", "Charge Sheet.csv"]:
        source_path = os.path.join(upload_dir, source_filename)
        if not os.path.exists(source_path):
            print(f"File '{source_filename}' does not exist at '{source_path}'")
            # You can choose to raise an error or continue with the next file.
            continue
        if not source_filename.endswith(".csv"):
            print(
                f"Error loading '{source_filename}': Unsupported format. Supported formats are: .csv"
            )
            # You can choose to raise an error or continue with the next file.
            continue
        try:
            # Read the CSV file using pandas
            df = pd.read_csv(source_path)
            # Create a new worksheet for the CSV data
            ws = combined_wb.create_sheet(title=source_filename.replace(".csv", ""))
            # Write the header as the first row
            ws.append(list(df.columns))
            # Convert the CSV data to rows and write to the worksheet
            for index, row in df.iterrows():
                row_values = list(row)
                ws.append(row_values)
        except Exception as e:
            print(f"Error loading '{source_filename}': {str(e)}")
            # You can choose to raise an error or continue with the next file.
            continue

    # Save the combined workbook
    combined_wb.save(combined_file_path)
    combined_wb.close()

    # Process data using the DataProcessor class
    file_paths = [
        "./uploads/Pay Journal (CSV).csv",
        f"./{combined_file_path}",
    ]
    data_processor = DataProcessor(file_paths)
    data_processor.process_data()
    # Provide the folder path and the CSV file name
    csv_folder_path = "./invoice_folder"
    pdf_folder_path = "./final_folder"
    # Process invoices using the InvoiceProcessor class
    invoice_folder = "invoice_folder"
    output_folder = "final_folder"
    os.makedirs(output_folder, exist_ok=True)
    invoice_processor = InvoiceProcessor(invoice_folder, output_folder)
    invoice_processor.process_invoices()
    pdf_folder = "final_folder"  # Specify the URL path where PDFs are served
    pdf_urls = []
    for pdf_filename in os.listdir(output_folder):
        pdf_path = f"{base_url}/{pdf_folder}/{pdf_filename}"
        pdf_urls.append(pdf_path)

    # Call the send_data_to_webhook function to send the data to the webhook
    # Organize the data and files into dictionaries
    result, files_list = calculate_amount_sum(csv_folder_path, pdf_folder_path)
    data = {"data": result}
    files = {"files": files_list}

    # Call the send_data_to_webhook function
    webhook_response = send_data_to_webhook(data, files)

    return {
        "message": "Data processing and invoice processing complete",
        "webhook_response": webhook_response,
        "pdf_urls": pdf_urls,
    }


@app.get("/{pdf_folder}/{pdf_filename}")
def serve_pdf(pdf_filename: str):
    pdf_path = os.path.join(pdf_folder, pdf_filename)
    if os.path.exists(pdf_path):
        return FileResponse(pdf_path)
    else:
        return {"detail": "PDF not found"}, 404
