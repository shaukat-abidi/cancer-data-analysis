import os
import sys
import pandas as pd
import yaml
from sqlalchemy import create_engine

# Add the directory containing utils.py to the Python path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../../utils")))

# Import the utility function
from utils import categorize_column, load_dataframe, load_yaml, display_dataset_info, func_rename_and_cast_columns, remove_duplicates, save_dataframe, check_file_exists, validate_categorical_columns


def main():
    # Filepaths
    filepath_patients_csv = "../../data/processed_data/processed_patients.csv"
    filepath_csv = "../../data/raw_data/procedures.csv"
    filepath_output = "../../data/processed_data/processed_procedures.csv"
    filepath_yaml = "../../config/procedures.yaml"
    schema = "Playground_MH_Oncology"
    tablename = "test_procedures"
    

    # Check if the processed patients file exists
    check_file_exists(filepath_patients_csv, f"{filepath_patients_csv} does not exist. Please run etl_patients.py first.")

    # Load datasets
    df_patients = load_dataframe(filepath_patients_csv)
    df = load_dataframe(filepath_csv)

    # Load YAML column mappings
    dict_column_mappings = load_yaml(filepath_yaml)

    # Display initial dataset information
    display_dataset_info(df, "Displaying initial dataset.")

    # Standardize column names and cast data types
    df = func_rename_and_cast_columns(df, dict_column_mappings["columns"])

    # Remove duplicate rows
    df = remove_duplicates(df)

    # Validate categorical columns
    validate_categorical_columns(df, ["procedure_description", "reason_description_for_procedure"])

    # Logical Testing: Check dates
    valid_dates = df[df["stop_time"] >= df["start_time"]]
    invalid_dates = df[df["stop_time"] < df["start_time"]]
    print(f"Total entries: {len(df)}, Valid dates: {len(valid_dates)}, Invalid dates: {len(invalid_dates)}")

    # Length of Stay in hours
    df['length_of_procedure_in_hours'] = (df['stop_time'] - df['start_time']).dt.total_seconds() / 3600.0

    # Left join with patient data
    patient_columns_needed = ['id', 'birthdate', 'marital', 'race', 'ethnicity', 'gender', 'income', 'income_category']
    df_patients = df_patients[patient_columns_needed]
    print(f"Length of DataFrame BEFORE left join: {len(df)}")
    df = pd.merge(df, df_patients, how='left', left_on='patient_id', right_on='id')
    df.drop(["id"], axis=1, inplace=True)
    print(f"Length of DataFrame AFTER left join: {len(df)}")

    # Impute Age of Patient
    df['birthdate'] = pd.to_datetime(df['birthdate'], errors='coerce')
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df['age_of_patient'] = (df['start_time'] - df['birthdate']).dt.days / 365.0

    # Categorize Age
    age_bins = [0, 17, 65, float("inf")]
    age_labels = ["children", "adult", "senior"]

    # Create a new column for age
    df = categorize_column(df, "age_of_patient", "age_category", age_bins, age_labels)

    # Display initial dataset information
    display_dataset_info(df, "Displaying final dataset.")

    # Save the cleaned and transformed dataset
    # save_dataframe(df, filepath_output)

    # Get the connection (SQLAlchemy)
    odbc_driver = 'ODBC Driver 17 for SQL Server'
    server = 'server_name'
    database = 'database_name'
    username = 'username' 
    password = 'password'

    # Connection String
    connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={odbc_driver}'

    # Create the SQLAlchemy engine
    engine = create_engine(connection_string)

    # Test the connection (optional)
    try:
        conn = engine.connect()
        print("Connection successful!")
        # conn.close()
    except Exception as e:
        print("Connection failed:", e)

    # Insert into SQL
    print(f"Writing df_csv to: {schema}.{tablename}")
    df.to_sql(name=tablename, con=engine, if_exists='replace', index=False, schema=schema)


if __name__ == "__main__":
    main()


