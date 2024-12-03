import os
import sys
import pandas as pd
import yaml

# Add the directory containing utils.py to the Python path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../../utils")))

# Import the utility function
from utils import check_file_exists, load_yaml, display_dataset_info, func_rename_and_cast_columns, validate_categorical_columns, categorize_column 

def main():
    # Filepaths
    filepath_patients_csv = "../../data/processed_data/processed_patients.csv"
    filepath_csv = "../../data/raw_data/medications.csv"
    filepath_output = "../../data/processed_data/processed_medications.csv"
    filepath_yaml = "../../config/medications.yaml"

    # Check if the processed patients file exists
    check_file_exists(filepath_patients_csv, f"{filepath_patients_csv} does not exist. Please run etl_patients.py first.")

    # Load datasets
    df_patients = pd.read_csv(filepath_patients_csv)
    df = pd.read_csv(filepath_csv)

    # Load YAML column mappings
    dict_column_mappings = load_yaml(filepath_yaml)

    # Display initial dataset information
    display_dataset_info(df, "Initial Dataset Info:")

    # Standardize column names and clean data
    df = func_rename_and_cast_columns(df, dict_column_mappings["columns"])

    # Remove duplicates
    print(f"Total length of DataFrame BEFORE removing duplicates: {len(df)}")
    df = df.drop_duplicates()
    print(f"Total length of DataFrame AFTER removing duplicates: {len(df)}")

    # Validate categorical columns
    validate_categorical_columns(df, ["medicine_description", "reason_description_for_medication"])

    # Logical Testing: Check dates
    valid_dates = df[df["stop_time"] >= df["start_time"]]
    invalid_dates = df[df["stop_time"] < df["start_time"]]
    print(f"Total entries: {len(df)}, Valid dates: {len(valid_dates)}, Invalid dates: {len(invalid_dates)}")

    # Calculate medication duration
    df['length_of_medication_in_days'] = (df['stop_time'] - df['start_time']).dt.days / 365.0

    # Mark medication as ended or recovered
    df['medicine_ended_or_recovered'] = df['stop_time'].apply(lambda x: 0 if pd.isna(x) else 1)


    # Left join with patient data
    patient_columns_needed = ['id', 'birthdate', 'marital', 'race', 'ethnicity', 'gender', 'income', 'income_category']
    df_patients = df_patients[patient_columns_needed]
    print(f"len of df BEFORE left-join: {len(df)}")
    df = pd.merge(df, df_patients, how='left', left_on='patient_id', right_on='id')
    df.drop(['id'], axis=1, inplace=True)
    print(f"len of df AFTER left-join: {len(df)}")

    # Impute Age of Patient
    df['birthdate'] = pd.to_datetime(df['birthdate'], errors='coerce')
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df['age_of_patient'] = (df['start_time'] - df['birthdate']).dt.days / 365.0

    # Categorize age
    age_bins = [0, 17, 65, float("inf")]
    age_labels = ["children", "adult", "senior"]
    # Create a new column for age category
    df = categorize_column(df, "age_of_patient", "age_category", age_bins, age_labels)

    # Display initial dataset information
    display_dataset_info(df, "Final Dataset Info:")

    # Save the cleaned and transformed dataset
    try:
        df.to_csv(filepath_output, index=False)
        print(f"Processed data saved to {filepath_output}")
    except Exception as e:
        print(f"Error saving processed data: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
