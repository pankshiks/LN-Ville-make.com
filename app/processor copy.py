import os
import pandas as pd
import openpyxl as px
from datetime import datetime
from app.csv_reader import DataReader


class DataProcessor:
    def __init__(self, file_paths):
        self.file_paths = file_paths
        self.output_folder = "output_folder"
        self.invoice_folder = "invoice_folder"
        # clients_df = pd.read_csv("./data/clients_and_projects.csv")

    def process_csv(self, file_path):
        journal_data = DataReader(file_path)
        journal_df = journal_data.read_csv(skip_rows=1)
        journal_df.fillna(0, inplace=True)
        grouped_journal_df = journal_df.groupby("Cost Centre")
        return grouped_journal_df

    def process_xlsx(self, file_path):
        workbook = px.load_workbook(file_path, data_only=True)
        grouped_data = {}
        for sheet_name in ["Job_Classifications", "Charge Sheet"]:
            if sheet_name in workbook.sheetnames:
                journal_df = pd.read_excel(file_path, sheet_name=sheet_name)
                grouped_data[sheet_name] = journal_df.groupby("Job Classification")
        return grouped_data["Job_Classifications"], grouped_data["Charge Sheet"]

    def process_and_return_data(self):
        grouped_data = {}
        for file_path in self.file_paths:
            if file_path.endswith(".csv"):
                grouped_data[file_path] = self.process_csv(file_path)
            elif file_path.endswith((".xlsx", ".xlsm")):
                grouped_data[file_path] = self.process_xlsx(file_path)
            else:
                print(f"Unsupported file format for {file_path}. Skipping...")
        return grouped_data

    def create_output_folders(self):
        os.makedirs(self.output_folder, exist_ok=True)
        os.makedirs(self.invoice_folder, exist_ok=True)

    def process_data(self):
        self.create_output_folders()
        csv_data_directory = "./data"
        csv_file_name = "clients_and_projects.csv"
        organizations_data = "organizations.csv"

        csv_file_path = os.path.join(csv_data_directory, csv_file_name)
        organization_file_path = os.path.join(csv_data_directory, organizations_data)

        grouped_data = self.process_and_return_data()
        file_data = {}

        for file_path, grouped_df in grouped_data.items():
            if isinstance(grouped_df, pd.core.groupby.generic.DataFrameGroupBy):
                grouped_journal_df = pd.concat([group for name, group in grouped_df])
                file_data[file_path] = grouped_journal_df
            else:
                for sheet_name, sheet_data in enumerate(grouped_df):
                    grouped_journal_df = pd.concat(
                        [group for name, group in sheet_data]
                    )
                    file_data[f"{file_path}_{sheet_name}"] = grouped_journal_df

        # Process and generate invoices
        for file_path in file_data:
            data1 = file_data["./uploads/Pay Journal (CSV).csv"]
            data2 = file_data[
                "./uploads/CYP invoice query FY 24 Auto Reconciliation.xlsm_0"
            ]
            data3 = file_data[
                "./uploads/CYP invoice query FY 24 Auto Reconciliation.xlsm_1"
            ]

            column_mapping = {
                "Employee Number": "Employee No.",
                "First Name": "Given Names",
            }

            data2 = data2.rename(columns=column_mapping)
            merged_data = data2.merge(
                data1, on=["Employee No.", "Last Name"], how="inner"
            )
            merged_data = merged_data.drop(columns="Given Names_y")
            merged_data = merged_data.rename(columns={"Given Names_x": "Given Names"})
            unique_cost_centre = merged_data["Cost Centre"].unique()

            # Create separate CSV files for each unique Cost Centre
            for cost_centre in unique_cost_centre:
                filtered_data = merged_data[
                    merged_data["Cost Centre"] == cost_centre
                ].copy()

                filtered_data = filtered_data.merge(
                    data3, on=["Job Classification"], how="inner"
                )
                filtered_data.set_index("Employee No.", inplace=True)
                filename = f"{cost_centre.replace(' ', '_')}.csv"
                file_path = os.path.join(self.output_folder, filename)
                filtered_data.to_csv(file_path)

            # Define your mapping
            mapping = {
                "Normal Hourly (Qty)": "NT",
                "Overtime 2.0 (Qty)": "OT",
                "AMWU - Meal Allowance (Meal)": "Overtime Meal Allowance",
                "Site Allowance - VIC (Qty)": "Site Allowance",
                "AMWU - Travel & Fares (Travel)": "Travel & Fares Allowance",
                "Overtime Productivity Allowance-VIC (Qty)": "Overtime Productivity Allowance",
                "Overtime 1.8 (Qty)": "OT",
                "Nightshift 1.8 (Qty)": "NT Shift",
                "Night Shift 200% (Qty)": "NT Shift",
                "AWU - Travel Allowance (Travel)": "Travel & Fares Allowance",
                "AWU - Overtime Meal Allowance (Meal)": "Overtime Meal Allowance",
                "Rain Work 1.0 (Qty)": "NT",
            }

            # Create a list of CSV files in the output folder
            output_folder = "output_folder"
            csv_files = [f for f in os.listdir(output_folder) if f.endswith(".csv")]

            for csv_file in csv_files:
                # Read data from the CSV file in the output folder
                data = pd.read_csv(os.path.join(output_folder, csv_file), header=[0])

                final_data = []
                total_amount = 0
                prefix = csv_file.split("-")
                for src_col, target_col in mapping.items():
                    if src_col in data.columns and target_col in data.columns:
                        unit = data[src_col].values
                        rate = data[target_col].values
                        if not any(unit) or not any(rate):
                            continue

                        result = unit * rate
                        description = (
                            data["Job Classification"]
                            + "-"
                            + prefix[0]
                            + "-"
                            + target_col
                            + "-"
                            + data["Given Names"]
                            + "-"
                            + data["Last Name"]
                        )

                        data["Given Names"] = data["Given Names"].values
                        data["Cost Centre"] = data["Cost Centre"].values
                        data["Last Name"] = data["Last Name"].values
                        split_names = data["Payroll Name Selection"].str.split(
                            "-", expand=True
                        )

                        period_end_date = datetime.strptime(
                            data["Period End Date"].iloc[0], "%d/%m/%Y"
                        )
                        serviced_start_date = period_end_date - pd.DateOffset(days=6)
                        serviced_period = f"{serviced_start_date.strftime('%d/%m/%Y')} - {period_end_date.strftime('%d/%m/%Y')}"
                        for i in range(len(result)):
                            serviced = serviced_period
                            unit_value = "{:.2f}".format(unit[i])
                            rate_value = "{:.2f}".format(rate[i])
                            amount = "{:.2f}".format(result[i])

                            total_amount += float(amount)

                            row = {
                                "Serviced": serviced,
                                "Description": description[i],
                                "Unit": unit_value,
                                "Rate": rate_value,
                                "Amount": amount,
                                "Given Names": data["Given Names"][i],
                                "Last Name": data["Last Name"][i],
                                "Cost Centre": data["Cost Centre"][i],
                                "Payroll Name": split_names[0][i],
                            }
                            final_data.append(row)

                result_df = pd.DataFrame(final_data)
                result_df = result_df.sort_values(by=["Given Names", "Last Name"])

                # Save the invoice for the current CSV file
                invoice_filename = os.path.splitext(csv_file)[0] + "_invoice.csv"
                invoice_filepath = os.path.join(self.invoice_folder, invoice_filename)
                result_df.to_csv(invoice_filepath, header=True, index=False)
