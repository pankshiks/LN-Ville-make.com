from app.processor import DataProcessor
from app.generate_pdf import InvoiceProcessor
import os


if __name__ == "__main__":
    file_paths = [
        "./data/Pay Journal (CSV).csv",
        "./data/CYP invoice query FY 24 Auto Reconciliation.xlsm",
    ]
    invoice_folder = 'invoice_folder'
    output_folder = 'final_folder'

    data_processor = DataProcessor(file_paths)
    data_processor.process_data()
    os.makedirs(output_folder, exist_ok=True)

    invoice_processor = InvoiceProcessor(invoice_folder, output_folder)
    invoice_processor.process_invoices()
