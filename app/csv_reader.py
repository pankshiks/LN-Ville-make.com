import pandas as pd


class DataReader:
    def __init__(self, file_path):
        """
        Initializes the DataReader object with the specified file path.

        :param file_path: The path to the data file.
        """
        self.file_path = file_path

    def read_csv(self, skip_rows=1):
        """
        Reads a CSV file and returns a Pandas DataFrame.

        :param skip_rows: Number of rows to skip from the beginning of the file.
        :return: Pandas DataFrame containing the CSV data.
        """
        try:
            data = pd.read_csv(self.file_path, skiprows=skip_rows)
            return data
        except Exception as e:
            return f"Error reading CSV: {str(e)}"

    def read_excel(self, sheet_name=None, skip_rows=1):
        """
        Reads an Excel file and returns a Pandas DataFrame.

        :param sheet_name: Name of the sheet to read from (default is None).
        :param skip_rows: Number of rows to skip from the beginning of the sheet.
        :return: Pandas DataFrame containing the Excel data.
        """
        try:
            data = pd.read_excel(self.file_path, sheet_name=sheet_name, skiprows=skip_rows)
            return data
        except Exception as e:
            return f"Error reading XLSX: {str(e)}"
