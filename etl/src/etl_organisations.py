import os
import sys
import pandas as pd
import yaml

# Add the directory containing utils.py to the Python path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../../utils")))

# Import the utility function
from utils import load_dataframe, load_yaml, display_dataset_info, func_rename_and_cast_columns, remove_duplicates, save_dataframe


def main():
    # Filepaths
    filepath_csv = "../../data/raw_data/organizations.csv"
    output_path = "../../data/processed_data/processed_organizations.csv"
    filepath_yaml = "../../config/organizations.yaml"

    # Load data
    df = load_dataframe(filepath_csv)

    # Load YAML column mappings
    dict_column_mappings = load_yaml(filepath_yaml)

    # Display initial dataset information
    display_dataset_info(df, "Displaying initial dataset.")

    # Standardize column names and cast data types
    df = func_rename_and_cast_columns(df, dict_column_mappings["columns"])

    # Remove duplicate rows
    df = remove_duplicates(df)

    # Save the cleaned and transformed dataset
    save_dataframe(df, output_path)


if __name__ == "__main__":
    main()
