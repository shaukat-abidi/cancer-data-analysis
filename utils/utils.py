"""
utils.py: A utility module for data preprocessing functions.

This module contains a set of utility functions for common data preprocessing
tasks such as renaming and casting columns in a pandas DataFrame.

"""

import pandas as pd
from ydata_profiling import ProfileReport

import yaml 

def func_generate_report(df, filename_of_report):
    # Calculate the size of the DataFrame in gigabytes
    size_gb = df.memory_usage(deep=True).sum() / (1024**3)
    print(f"Size of the DataFrame: {size_gb:.2f} GB")
    # Create report
    profile = ProfileReport(df, title="Report")
    profile.to_file(filename_of_report)
    

def func_return_mappings(filename_yaml):
    with open(filename_yaml, "r") as file:
        dict_mappings = yaml.safe_load(file)
    return dict_mappings

def func_cast_columns(df, column_mappings):
    """
    Casts column types in a pandas DataFrame.

    This function casts each column to its specified data type.

    Parameters:
    -----------
    df : pd.DataFrame
        The input pandas DataFrame to process.

    column_mappings : dict
        A dictionary containing mappings for column names and types.
        Example format:
        {
            "normalised_colname": {
                "type": "str" | "float" | "datetimestamp"
            }
        }

    Returns:
    --------
    pd.DataFrame
        The modified pandas DataFrame with applied types.

    Raises:
    -------
    KeyError:
        If a column specified in `column_mappings` is not found in the DataFrame.
    ValueError:
        If a column cannot be cast to the specified type.

    Notes:
    ------
    - For columns of type 'str', missing or NaN values are replaced with 'Unknown'.
    - For columns of type 'datetimestamp', invalid dates are coerced to NaT.
    - For columns of type 'float', invalid values are coerced to NaN.
    """

    # Ensure column_mappings is not empty
    if not column_mappings:
        raise ValueError("column_mappings cannot be empty.")

    # Process each column based on the mappings
    for original_col in df.columns.tolist():
        if original_col not in column_mappings.keys():
            print(f"{original_col} not found in column mappings")
            continue  # Skip columns not in the mapping

        try:
            # Extract normalized column name and type
            mapping = column_mappings[original_col]
            col_type = mapping["type"]

            # Handle type casting
            if col_type == "str":
                df[original_col] = df[original_col].fillna("Unknown").astype(str)
                print(f"Column '{original_col}' cast to type 'str'.")
            elif col_type == "datetimestamp":
                df[original_col] = pd.to_datetime(df[original_col], errors="coerce").dt.tz_localize(None)
                print(f"Column '{original_col}' cast to type 'datetimestamp'.")
            elif col_type == "float":
                df[original_col] = pd.to_numeric(df[original_col], errors="coerce")
                print(f"Column '{original_col}' cast to type 'numeric'.")
            else:
                raise ValueError(f"Invalid type '{col_type}' for column '{original_col}'.")

            print("-" * 20)

        except KeyError as e:
            raise KeyError(f"Column '{original_col}' is missing required mappings: {e}")
        except Exception as e:
            raise ValueError(f"Error processing column '{original_col}': {e}")

    return df

def func_rename_and_cast_columns(df, column_mappings):
    """
    Standardizes column names and casts column types in a pandas DataFrame.

    This function renames columns of a DataFrame based on the provided
    column mappings and casts each column to its specified data type.

    Parameters:
    -----------
    df : pd.DataFrame
        The input pandas DataFrame to process.

    column_mappings : dict
        A dictionary containing mappings for column names and types.
        Example format:
        {
            "original_column_name": {
                "normalised_colname": "new_column_name",
                "type": "str" | "float" | "datetimestamp"
            }
        }

    Returns:
    --------
    pd.DataFrame
        The modified pandas DataFrame with standardized column names and types.

    Raises:
    -------
    KeyError:
        If a column specified in `column_mappings` is not found in the DataFrame.
    ValueError:
        If a column cannot be cast to the specified type.

    Notes:
    ------
    - For columns of type 'str', missing or NaN values are replaced with 'Unknown'.
    - For columns of type 'datetimestamp', invalid dates are coerced to NaT.
    - For columns of type 'float', invalid values are coerced to NaN.
    """

    # Ensure column_mappings is not empty
    if not column_mappings:
        raise ValueError("column_mappings cannot be empty.")

    # Process each column based on the mappings
    for original_col in df.columns.tolist():
        if original_col not in column_mappings.keys():
            print(f"{original_col} not found in column mappings")
            continue  # Skip columns not in the mapping

        try:
            # Extract normalized column name and type
            mapping = column_mappings[original_col]
            new_col = mapping["normalised_colname"]
            col_type = mapping["type"]

            # Rename the column
            df.rename(columns={original_col: new_col}, inplace=True)
            print(f"Renamed column: '{original_col}' to '{new_col}'.")

            # Handle type casting
            if col_type == "str":
                df[new_col] = df[new_col].fillna("Unknown").astype(str)
                print(f"Column '{new_col}' cast to type 'str'.")
            elif col_type == "datetimestamp":
                df[new_col] = pd.to_datetime(df[new_col], errors="coerce").dt.tz_localize(None)
                print(f"Column '{new_col}' cast to type 'datetimestamp'.")
            elif col_type == "float":
                df[new_col] = pd.to_numeric(df[new_col], errors="coerce")
                print(f"Column '{new_col}' cast to type 'numeric'.")
            else:
                raise ValueError(f"Invalid type '{col_type}' for column '{new_col}'.")

            print("-" * 20)

        except KeyError as e:
            raise KeyError(f"Column '{original_col}' is missing required mappings: {e}")
        except Exception as e:
            raise ValueError(f"Error processing column '{original_col}': {e}")

    return df
