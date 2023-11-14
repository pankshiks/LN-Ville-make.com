from fastapi import FastAPI, UploadFile, HTTPException
from app.processor import DataProcessor
from app.generate_pdf import InvoiceProcessor
from pydantic import BaseModel
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.utils import get_openapi
from typing import List
import os
import openpyxl
import pandas as pd
import requests
import json


class AppConfig:
    BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
    DIRECTORY_PATH = "final_folder"
    ONE_MORE = "invoice_folder"
    CSV_DATA_DIRECTORY = "./data"
    CSV_FILE_NAME = "clients_and_projects.csv"
    CLIENTS_DATA = "map_clients.csv"
    ORGANIZATIONS_DATA = "organizations.csv"


app = FastAPI(swagger_ui_parameters={"defaultModelsExpandDepth": -1}, redoc_url=None)

app_settings = AppConfig()

# Check if the directory exists, and create it if it doesn't
for directory in [app_settings.DIRECTORY_PATH, app_settings.ONE_MORE]:
    os.makedirs(directory, exist_ok=True)

# Mount the directories as static directories
app.mount("/pdfs", StaticFiles(directory=app_settings.DIRECTORY_PATH), name="pdfs")
pdf_folder = "final_folder"  # This is the folder where your PDFs are stored


class FileExtensionValidator:
    @staticmethod
    def is_valid_file_extension(filename, valid_extensions):
        ext = filename.split(".")[-1]
        return ext in valid_extensions


class WebhookSender:
    @staticmethod
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


class CsvProcessor:
    @staticmethod
    def partial_match(row, organizations_df):
        cleaned_payroll_name = row["Payroll Name"].replace(" ", "").lower()
        cleaned_contract_entity = (
            organizations_df["Contract Entity"].str.replace(" ", "").str.lower()
        )

        matching_rows = organizations_df[
            cleaned_contract_entity.str.contains(
                cleaned_payroll_name, case=False, regex=False
            )
        ]
        return matching_rows.iloc[0]
    
    @staticmethod
    def calculate_amount_sum(csv_folder_path, pdf_folder_path):
        try:
            results = []
            files_list = []

            for file_name in os.listdir(csv_folder_path):
                file_path = os.path.join(csv_folder_path, file_name)
                df = pd.read_csv(file_path)
                clients_file_path = os.path.join(
                    app_settings.CSV_DATA_DIRECTORY, app_settings.CLIENTS_DATA
                )
                organization_file_path = os.path.join(
                    app_settings.CSV_DATA_DIRECTORY, app_settings.ORGANIZATIONS_DATA
                )

                find_clients_df = pd.read_csv(clients_file_path)
                organizations_df = pd.read_csv(organization_file_path)

                matching_rows_df = df.apply(
                    CsvProcessor.partial_match, axis=1, organizations_df=organizations_df
                )

                if not matching_rows_df.empty:
                    filtered_data = matching_rows_df.iloc[0].to_dict()
                    total_amount = df["Amount"].sum()
                    index_cost_centre = df["Cost Centre"].iloc[0]
                    search_entity = find_clients_df.loc[
                        find_clients_df["Cost Centre"] == index_cost_centre,
                        "Search Entity",
                    ].values[0]

                    result = {
                        "total_amount": total_amount,
                        "filtered_data": filtered_data,
                        "client_name": search_entity,
                    }
                    results.append(result)

                    pdf_file_name = os.path.splitext(file_name)[0] + ".pdf"
                    pdf_file_path = os.path.join(pdf_folder_path, pdf_file_name)

                    with open(pdf_file_path, "rb") as pdf_file:
                        pdf_binary = pdf_file.read()

                    files = (pdf_file_name, pdf_binary, "application/pdf")
                    files_list.append(files)

            return results, files_list
        except Exception as e:
            print("Not Working", e)
            return None


class ProcessInvoicesResponse(BaseModel):
    message: str
    pdf_urls: List[str]


validator = FileExtensionValidator()
webhook_sender = WebhookSender()
csv_processor = CsvProcessor()


# Redirect the root path to /docs
@app.get("/", include_in_schema=False)
async def redirect_to_docs():
    return RedirectResponse(url="/docs")


@app.post("/process_data_and_invoices", response_model=ProcessInvoicesResponse, tags=["Run Script"])
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

    for file in files:
        if not validator.is_valid_file_extension(file.filename, valid_extensions):
            raise HTTPException(
                status_code=400,
                detail="Invalid file extension. Supported extensions are .csv and .xlsx.",
            )

    upload_dir = "uploads"
    os.makedirs(upload_dir, exist_ok=True)

    for i, file in enumerate(files):
        content = await file.read()
        filename = file_names[i]
        with open(os.path.join(upload_dir, filename), "wb") as f:
            f.write(content)

    combined_file_path = os.path.join(
        upload_dir, "CYP invoice query FY 24 Auto Reconciliation.xlsm"
    )
    combined_wb = openpyxl.Workbook()

    for source_filename in ["Job_Classifications.csv", "Charge Sheet.csv"]:
        source_path = os.path.join(upload_dir, source_filename)
        if not os.path.exists(source_path):
            print(f"File '{source_filename}' does not exist at '{source_path}'")
            continue
        if not source_filename.endswith(".csv"):
            print(
                f"Error loading '{source_filename}': Unsupported format. Supported formats are: .csv"
            )
            continue
        try:
            df = pd.read_csv(source_path)
            ws = combined_wb.create_sheet(title=source_filename.replace(".csv", ""))
            ws.append(list(df.columns))
            for index, row in df.iterrows():
                row_values = list(row)
                ws.append(row_values)
        except Exception as e:
            print(f"Error loading '{source_filename}': {str(e)}")
            continue

    combined_wb.save(combined_file_path)
    combined_wb.close()

    file_paths = [
        "./uploads/Pay Journal (CSV).csv",
        f"./{combined_file_path}",
    ]
    data_processor = DataProcessor(file_paths)
    data_processor.process_data()

    csv_folder_path = "./invoice_folder"
    pdf_folder_path = "./final_folder"
    invoice_folder = "invoice_folder"
    output_folder = "final_folder"
    os.makedirs(output_folder, exist_ok=True)
    invoice_processor = InvoiceProcessor(invoice_folder, output_folder)
    invoice_processor.process_invoices()
    pdf_folder = "final_folder"
    pdf_urls = []
    for pdf_filename in os.listdir(output_folder):
        pdf_path = f"{app_settings.BASE_URL}/{pdf_folder}/{pdf_filename}"
        pdf_urls.append(pdf_path)

    result, files_list = csv_processor.calculate_amount_sum(
        csv_folder_path, pdf_folder_path
    )
    data = {"data": result}
    files = {"files": files_list}

    webhook_response = webhook_sender.send_data_to_webhook(data, files)

    return {
        "message": "Data processing and invoice processing complete",
        "webhook_response": webhook_response,
        "pdf_urls": pdf_urls,
    }


@app.get("/{pdf_folder}/{pdf_filename}", include_in_schema=False)
def serve_pdf(pdf_filename: str):
    pdf_path = os.path.join(app_settings.DIRECTORY_PATH, pdf_filename)
    if os.path.exists(pdf_path):
        return FileResponse(pdf_path)
    else:
        return {"detail": "PDF not found"}, 404


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="Automation Script",
        version="1.0.0",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
