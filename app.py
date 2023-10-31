from config.csv_reader import DataReader
import pandas as pd
import openpyxl as px
import os
from datetime import datetime, timedelta


class DataProcessor:
    def __init__(self, file_paths):
        self.file_paths = file_paths

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


if __name__ == "__main__":
    file_data = {}
    file_paths = [
        "./data/Pay Journal (CSV).csv",
        "./data/CYP invoice query FY 24 Auto Reconciliation.xlsm",
    ]

    data_processor = DataProcessor(file_paths)
    grouped_data = data_processor.process_and_return_data()

    for file_path, grouped_df in grouped_data.items():
        if isinstance(grouped_df, pd.core.groupby.generic.DataFrameGroupBy):
            grouped_journal_df = pd.concat([group for name, group in grouped_df])
            file_data[file_path] = grouped_journal_df
        else:
            for sheet_name, sheet_data in enumerate(grouped_df):
                grouped_journal_df = pd.concat([group for name, group in sheet_data])
                file_data[f"{file_path}_{sheet_name}"] = grouped_journal_df

    data1 = file_data["./data/Pay Journal (CSV).csv"]
    data2 = file_data["./data/CYP invoice query FY 24 Auto Reconciliation.xlsm_0"]
    data3 = file_data["./data/CYP invoice query FY 24 Auto Reconciliation.xlsm_1"]

    column_mapping = {
        "Employee Number": "Employee No.",
        "First Name": "Given Names",
    }

    # Create a folder to store the CSV files
    output_folder = "output_folder"
    os.makedirs(output_folder, exist_ok=True)
    # Create a folder to store the CSV files
    invoice_folder = "invoice_folder"
    os.makedirs(invoice_folder, exist_ok=True)

    data2 = data2.rename(columns=column_mapping)
    merged_data = data2.merge(data1, on=["Employee No.", "Last Name"], how="inner")

    merged_data = merged_data.drop(columns="Given Names_y")
    merged_data = merged_data.rename(columns={"Given Names_x": "Given Names"})
    unique_cost_centre = merged_data["Cost Centre"].unique()

    # Create separate CSV files for each unique Cost Centre
    for cost_centre in unique_cost_centre:
        filtered_data = merged_data[merged_data["Cost Centre"] == cost_centre].copy()
        filtered_data = filtered_data.merge(
            data3, on=["Job Classification"], how="inner"
        )
        filtered_data.set_index("Employee No.", inplace=True)

        filename = f"{cost_centre.replace(' ', '_')}.csv"  # Create a filename based on Job Classification
        file_path = os.path.join(output_folder, filename)  # Specify the folder path
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
                    + target_col
                    + "-"
                    + data["Given Names"]
                    + "-"
                    + data["Last Name"]
                )

                # Add Given Names and Last Name columns
                data["Given Names"] = data["Given Names"].values
                data["Last Name"] = data["Last Name"].values
                period_end_date =  datetime.strptime(data["Period End Date"].iloc[0], "%d/%m/%Y")
                serviced_start_date = period_end_date - pd.DateOffset(days=6)
                serviced_period = f"{serviced_start_date.strftime('%d/%m/%Y')} - {period_end_date.strftime('%d/%m/%Y')}"

                for i in range(len(result)):
                    serviced = serviced_period  # Assuming "serviced" is empty
                    unit_value = "{:.2f}".format(
                        unit[i]
                    )  # Format with 2 decimal places
                    rate_value = "{:.2f}".format(
                        rate[i]
                    )  # Format with 2 decimal places
                    amount = "{:.2f}".format(result[i])  # Format with 2 decimal places

                    # Add the current amount to the total
                    total_amount += float(amount)

                    row = {
                        "Serviced": serviced,
                        "Description": description[i],
                        "Unit": unit_value,
                        "Rate": rate_value,
                        "Amount": amount,
                        "Given Names": data["Given Names"][i],
                        "Last Name": data["Last Name"][i],
                    }

                    final_data.append(row)

        result_df = pd.DataFrame(final_data)
        result_df = result_df.sort_values(by=["Given Names", "Last Name"])

        # Save the invoice for the current CSV file
        invoice_filename = os.path.splitext(csv_file)[0] + "_invoice.csv"
        invoice_filepath = os.path.join(invoice_folder, invoice_filename)
        result_df.to_csv(invoice_filepath, header=True, index=False)
