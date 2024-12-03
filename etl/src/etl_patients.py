import os
import sys
import pandas as pd
import yaml

# Add the directory containing utils.py to the Python path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../../utils")))

# Import the utility function
from utils import func_rename_and_cast_columns, load_yaml, display_dataset_info, validate_categorical_columns, categorize_column

def main():
    # Filepaths
    filepath_csv = "../../data/raw_data/patients.csv"
    output_path = "../../data/processed_data/processed_patients.csv"
    filepath_yaml = "../../config/patients.yaml"

    # Load dataset
    try:
        df_patients = pd.read_csv(filepath_csv)
        print(f"CSV loaded into dataframe from: {df_patients}")
    except FileNotFoundError:
        print(f"Error: File not found at {filepath_csv}")
        sys.exit(1)

    # Load column mappings from YAML
    dict_column_mappings = load_yaml(filepath_yaml)

    # Display initial dataset information
    display_dataset_info(df_patients, "Initial Dataset Info:")

    # Clean and cast columns
    df_patients = func_rename_and_cast_columns(df_patients, dict_column_mappings['columns'])

    # Check for duplicate rows
    initial_len = len(df_patients)
    print(f"Total length of DataFrame BEFORE removing duplicates: {initial_len}")
    df_patients = df_patients.drop_duplicates()
    print(f"Total length of DataFrame AFTER removing duplicates: {len(df_patients)}")

    # Validate categorical columns
    categorical_columns = ['marital', 'race', 'ethnicity', 'gender']
    validate_categorical_columns(df_patients, categorical_columns)

    # Categorize income
    income_bins = [0, 45000, 90000, float("inf")]
    income_labels = ["low-income", "medium-income", "high-income"]
    df_patients = categorize_column(df=df_patients, column_name="income", categorised_column_name="income_category", bins=income_bins, labels=income_labels)
    
    # display processed dataset
    display_dataset_info(df_patients, "processed Dataset Info:")

    # Save the cleaned and transformed dataset
    try:
        df_patients.to_csv(output_path, index=False)
        print(f"Processed data saved to {output_path}")
    except Exception as e:
        print(f"Error saving processed data: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
