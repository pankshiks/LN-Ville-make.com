import os
import pandas as pd
import pdfkit
from jinja2 import Environment, FileSystemLoader


class InvoiceProcessor:
    def __init__(self, invoice_folder, output_folder):
        self.invoice_folder = invoice_folder
        self.output_folder = output_folder

    def process_invoices(self):
        invoice_files = [f for f in os.listdir(self.invoice_folder) if f.endswith('_invoice.csv')]
        gst_rate = 0.10

        for invoice_file in invoice_files:
            data = pd.read_csv(os.path.join(self.invoice_folder, invoice_file), header=[0])
            data = data[data["Amount"] != 0]

            data["Amount"] = data["Amount"].apply(lambda x: "{:.2f}".format(float(x)) if x else "0.00")

            subtotal = data["Amount"].astype(float).sum()
            gst = float(subtotal) * gst_rate

            # Ensure two decimal places for subtotal and gst
            subtotal = "{:.2f}".format(subtotal)
            gst = "{:.2f}".format(gst)

            grand_total = float(subtotal) + float(gst)

            totals = {
                "subtotal": subtotal,
                "gst": gst,
                "grand_total": "{:.2f}".format(grand_total),
            }

            result_df_with_blanks = pd.DataFrame()
            current_given_names = None

            blank_row = {
                "Serviced": "",
                "Description": "",
                "Unit": "",
                "Rate": "",
                "Amount": "",
            }

            for index, row in data.iterrows():
                given_names = row["Given Names"]
                if current_given_names is None or given_names != current_given_names:
                    result_df_with_blanks = pd.concat(
                        [result_df_with_blanks, pd.DataFrame(blank_row, index=[0])]
                    )
                result_df_with_blanks = pd.concat([result_df_with_blanks, pd.DataFrame(row).T])
                current_given_names = given_names

            # Create a Jinja2 environment and load the template
            env = Environment(loader=FileSystemLoader("."))
            template = env.get_template("templates/template.html")
            # Render the template with your data
            rendered_html = template.render(data=result_df_with_blanks, totals=totals)
            # Define output PDF file name based on the current invoice file
            pdf_output = os.path.join(self.output_folder, os.path.splitext(invoice_file)[0] + ".pdf")
            # Use pdfkit to generate PDF from rendered HTML
            pdfkit.from_string(rendered_html, pdf_output)



