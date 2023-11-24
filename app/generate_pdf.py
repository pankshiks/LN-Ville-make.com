import os
import pandas as pd
import pdfkit
from jinja2 import Environment, FileSystemLoader
from concurrent.futures import ThreadPoolExecutor


csv_data_directory = "./data"
csv_file_name = "clients_and_projects.csv"
clients_data = "map_clients.csv"
organizations_data = "organizations.csv"


def partial_match(row, organizations_df):
    """
    Perform a partial match on "Payroll Name" and "Contract Entity" columns.

    :param row: DataFrame row containing "Payroll Name" to match.
    :param organizations_df: DataFrame containing "Contract Entity" for matching.
    :return: Matching row from organizations_df.
    """
    cleaned_payroll_name = row["Payroll Name"].replace(" ", "").lower()
    cleaned_contract_entity = organizations_df["Contract Entity"].str.replace(" ", "").str.lower()
    matching_rows = organizations_df[cleaned_contract_entity.str.contains(cleaned_payroll_name, case=False, regex=False)]
    return matching_rows.iloc[0]


class InvoiceProcessor:
    def __init__(self, invoice_folder, output_folder):
        """
        Initialize the InvoiceProcessor with input and output folders.

        :param invoice_folder: Folder containing input invoice files.
        :param output_folder: Folder to store generated PDFs.
        """
        self.invoice_folder = invoice_folder
        self.output_folder = output_folder

    def generate_pdf(self, invoice_file):
        """
        Generate a PDF invoice from a CSV file.

        :param invoice_file: Name of the input CSV invoice file.
        """
        try:
            # Read CSV data and preprocess
            data = pd.read_csv(os.path.join(self.invoice_folder, invoice_file), header=[0])
            data = data[data["Amount"] != 0]
            data["Amount"] = data["Amount"].apply(lambda x: "{:.2f}".format(float(x)) if x else "0.00")

            # Calculate totals
            subtotal = data["Amount"].astype(float).sum()
            gst_rate = 0.10
            gst = float(subtotal) * gst_rate
            subtotal, gst = "{:.2f}".format(subtotal), "{:.2f}".format(gst)
            grand_total = float(subtotal) + float(gst)
            totals = {"subtotal": subtotal, "gst": gst, "grand_total": "{:.2f}".format(grand_total)}

            # Prepare data for rendering
            result_df_with_blanks = pd.DataFrame()
            current_given_names = None
            blank_row = {"Serviced": "", "Description": "", "Unit": "", "Rate": "", "Amount": ""}

            for index, row in data.iterrows():
                given_names = row["Given Names"]
                if current_given_names is None or given_names != current_given_names:
                    result_df_with_blanks = pd.concat([result_df_with_blanks, pd.DataFrame(blank_row, index=[0])])
                result_df_with_blanks = pd.concat([result_df_with_blanks, pd.DataFrame(row).T])
                current_given_names = given_names

            # Load Jinja2 template
            env = Environment(loader=FileSystemLoader("."))
            template = env.get_template("templates/template.html")

            # Load CSV data for matching
            organization_file_path = os.path.join(csv_data_directory, organizations_data)
            clients_file_path = os.path.join(csv_data_directory, clients_data)
            organizations_df = pd.read_csv(organization_file_path)
            clients_df = pd.read_csv(clients_file_path)
            cost_centre = invoice_file.split("_")
            filtered_data = clients_df[clients_df["Cost Centre"] == cost_centre[0]]

            # Perform partial match and render HTML
            organizations_df.columns = organizations_df.columns.str.strip()
            matching_rows_df = data.apply(partial_match, axis=1, organizations_df=organizations_df)
            rendered_html = template.render(data=result_df_with_blanks,
                                           totals=totals,
                                           additional_info=matching_rows_df.iloc[0],
                                           invoice_info=filtered_data.iloc[0])

            # Define output PDF file name based on the current invoice file
            pdf_output = os.path.join(self.output_folder, os.path.splitext(invoice_file)[0] + ".pdf")

            # Use pdfkit to generate PDF from rendered HTML
            pdfkit.from_string(rendered_html, pdf_output)
            print(f"PDF generated successfully for {invoice_file}")

        except Exception as e:
            print(f"Error generating PDF for {invoice_file}: {e}")

    def process_invoices(self):
        """
        Process all CSV invoices in the specified folder using ThreadPoolExecutor.
        """
        invoice_files = [f for f in os.listdir(self.invoice_folder) if f.endswith("_invoice.csv")]

        with ThreadPoolExecutor(max_workers=1) as executor:  # Adjust max_workers as needed
            executor.map(self.generate_pdf, invoice_files)
