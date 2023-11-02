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

app = FastAPI()
app.mount("/pdfs", StaticFiles(directory="final_folder"), name="pdfs")


def is_valid_file_extension(filename, valid_extensions):
    ext = filename.split(".")[-1]
    return ext in valid_extensions


class ProcessInvoicesResponse(BaseModel):
    message: str
    pdf_urls: List[str]


@app.post("/process_data")
async def process_data(
    pay_journal: UploadFile,
    daily_cost_detail: UploadFile,
    input_charge_Sheet: UploadFile,
    job_classification: UploadFile,
):
    valid_extensions = ["csv", "xlsx"]

    files = [
        pay_journal,
        daily_cost_detail,
        input_charge_Sheet,
        job_classification
    ]

    file_names = [
        "Pay Journal (CSV).csv",
        "Daily Cost Detail - Actual (CSV).csv",
        "Charge Sheet.csv",
        "Job_Classifications.csv"
    ]

    # Check file extensions for validity
    for file in files:
        if not is_valid_file_extension(file.filename, valid_extensions):
            raise HTTPException(status_code=400, detail="Invalid file extension. Supported extensions are .csv and .xlsx.")

    # Define the directory where the files will be saved
    upload_dir = "uploads"
    os.makedirs(upload_dir, exist_ok=True)

    for i, file in enumerate(files):
        content = await file.read()
        filename = file_names[i]
        with open(os.path.join(upload_dir, filename), 'wb') as f:
            f.write(content)

    # Create a new workbook for the combined XLSX file
    combined_file_path = os.path.join(upload_dir, "CYP invoice query FY 24 Auto Reconciliation.xlsm")
    combined_wb = openpyxl.Workbook()
    
    for source_filename in ["Job_Classifications.csv", "Charge Sheet.csv"]:
        source_path = os.path.join(upload_dir, source_filename)

        if not os.path.exists(source_path):
            print(f"File '{source_filename}' does not exist at '{source_path}'")
            # You can choose to raise an error or continue with the next file.
            continue

        if not source_filename.endswith('.csv'):
            print(f"Error loading '{source_filename}': Unsupported format. Supported formats are: .csv")
            # You can choose to raise an error or continue with the next file.
            continue
        try:
            # Read the CSV file using pandas
            df = pd.read_csv(source_path)

            # Create a new worksheet for the CSV data
            ws = combined_wb.create_sheet(title=source_filename.replace('.csv', ''))
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

    return {"message": "Data processing complete"}


@app.post("/process_invoices", response_model=ProcessInvoicesResponse)
def process_invoices():
    invoice_folder = 'invoice_folder'
    output_folder = 'final_folder'
    os.makedirs(output_folder, exist_ok=True)

    invoice_processor = InvoiceProcessor(invoice_folder, output_folder)
    invoice_processor.process_invoices()

    pdf_folder = "final_folder"  # Specify the URL path where PDFs are served
    pdf_urls = []
    for pdf_filename in os.listdir(output_folder):
        pdf_url = f"http://127.0.0.1:8000/{pdf_folder}/{pdf_filename}"
        pdf_urls.append(pdf_url)

    return {"message": "Invoice processing complete", "pdf_urls": pdf_urls}


pdf_folder = "final_folder"  # This is the folder where your PDFs are stored


@app.get("/{pdf_folder}/{pdf_filename}")
def serve_pdf(pdf_filename: str):
    pdf_path = os.path.join(pdf_folder, pdf_filename)
    if os.path.exists(pdf_path):
        return FileResponse(pdf_path)
    else:
        return {"detail": "PDF not found"}, 404
