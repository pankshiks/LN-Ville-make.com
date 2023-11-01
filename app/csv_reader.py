import pandas as pd


class DataReader:
    def __init__(self, file_path):
        self.file_path = file_path

    def read_csv(self, skip_rows=1):
        try:
            data = pd.read_csv(self.file_path, skiprows=skip_rows)
            return data
        except Exception as e:
            return f"Error reading CSV: {str(e)}"

    def read_excel(self, sheet_name=None, skip_rows=1):
        try:
            data = pd.read_excel(self.file_path, sheet_name=sheet_name, skiprows=skip_rows)
            return data
        except Exception as e:
            return f"Error reading XLSX: {str(e)}"
