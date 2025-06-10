import sqlite3
import argparse
import pprint
import sys
import os
#from typing import Dict, List
#====================================================================
# README
#====================================================================
# Simple script to print a dict representation of a database.
# usage: py_get_DB_schema.py [-h] --path PATH [--count] [--list]
#
# Print a SQLite 3 database structure using a Python dict representation.
#
# options:
#  -h, --help            show this help message and exit
#  --path PATH, -p PATH  Path to SQLite3 database
#  --count, -c           List tables and their number of columns
#   --list, -l            List tables and their columns
#====================================================================


#====================================================================
# DATA
#====================================================================
C_TABLES_CAM_DB_W23H2_L = ['BinaryFullPaths', 
                         'Capabilities', 
                         'FileIDs', 
                         'NonPackagedIdentityRelationship', 
                         'NonPackagedUsageHistory', 
                         'PackageFamilyNames', 
                         'PackagedUsageHistory', 
                         'ProgramIDs', 
                         'Users']

C_TABLES_CAM_DB_W23H2_D = {'NonPackagedUsageHistory': ['AccessBlocked','BinaryFullPath','Capability','FileID','ID','LastUsedTimeStart', 'LastUsedTimeStop','ProgramID','UserSid'],
 'PackagedUsageHistory': ['AccessBlocked','Capability','ID','LastUsedTimeStart','LastUsedTimeStop','PackageFamilyName','UserSid'],
 'NonPackagedIdentityRelationship': ['BinaryFullPath', 'FileID', 'ID', 'LastObservedTime', 'ProgramID'],
 'Capabilities': ['ID','StringValue'],
 'PackageFamilyNames': ['ID','StringValue'],
 'BinaryFullPaths': ['ID','StringValue'],
 'Users': ['ID','StringValue'],
 'FileIDs': ['ID','StringValue'],
 'ProgramIDs': ['ID','StringValue']}

C_TABLES_CAM_DB_COLS_COUNT_W23H2_D ={'NonPackagedUsageHistory': 9,
                                     'PackagedUsageHistory': 7,
                                     'NonPackagedIdentityRelationship': 5,
                                     'Capabilities': 2,
                                     'PackageFamilyNames': 2,
                                     'BinaryFullPaths': 2,
                                     'Users': 2,
                                     'FileIDs': 2,
                                     'ProgramIDs': 2}

#====================================================================
# W24H2
#====================================================================
# Number of tables of CAM DB 
C_NUM_TABLES_CAM_DB_W24H2 = 13  # W11/24H2

C_TABLES_CAM_DB_W24H2_L = ['AccessGUIDs', 
                         'AppNames', 
                         'BinaryFullPaths', 
                         'Capabilities', 
                         'FileIDs', 
                         'NonPackagedGlobalPromptHistory', 
                         'NonPackagedIdentityRelationship', 
                         'NonPackagedUsageHistory', 
                         'PackageFamilyNames', 
                         'PackagedUsageHistory', 
                         'ProgramIDs', 
                         'ServiceNames', 
                         'Users']

C_TABLES_CAM_DB_W24H2_D = {'Capabilities': ['ID','StringValue'],
 'PackageFamilyNames': ['ID','StringValue'],
 'BinaryFullPaths': ['ID','StringValue'],
 'Users': ['ID', 'StringValue'],
 'FileIDs': ['ID', 'StringValue'],
 'ProgramIDs': ['ID', 'StringValue'],
 'AccessGUIDs': ['ID', 'StringValue'],
 'AppNames': ['ID', 'StringValue'],
 'ServiceNames': ['ID', 'StringValue'],
 'NonPackagedUsageHistory': ['AccessBlocked', 'AccessGUID', 'AppName', 'BinaryFullPath', 'Capability', 'FileID', 'ID', 'Label', 'LastUsedTimeStart', 'LastUsedTimeStop', 'ProgramID', 'ServiceName', 'UserSid'],
 'PackagedUsageHistory': ['AccessBlocked', 'AccessGUID', 'AppName', 'Capability', 'ID', 'Label', 'LastUsedTimeStart', 'LastUsedTimeStop', 'PackageFamilyName', 'UserSid'],
 'NonPackagedIdentityRelationship': ['BinaryFullPath', 'FileID', 'ID', 'LastObservedTime', 'ProgramID'],
 'NonPackagedGlobalPromptHistory': ['Capability', 'FileID', 'ID', 'ProgramID', 'ShownTime', 'UserSid']}


C_TABLES_CAM_DB_COLS_COUNT_W24H2_D = { 'Capabilities': 2,
                                       'PackageFamilyNames': 2,
                                       'BinaryFullPaths': 2,
                                       'Users': 2,
                                       'FileIDs': 2,
                                       'ProgramIDs': 2,
                                       'AccessGUIDs': 2,
                                       'AppNames': 2,
                                       'ServiceNames': 2,
                                       'NonPackagedUsageHistory': 13,
                                       'PackagedUsageHistory': 10,
                                       'NonPackagedIdentityRelationship': 5,
                                       'NonPackagedGlobalPromptHistory': 6}


#====================================================================
# CODE
#====================================================================
#--------------------------------------------------------------------
# Opens a SQLite3 database in read-only mode and retrieves the names of all
# tables and their corresponding field names, sorted alphabetically.
# @param db_path [IN]: File path to the SQLite3 database
# @return A dictionary where keys are the names of the tables in the database,
#        and values are lists containing the alphabetically sorted names of
#        the columns (fields) for each respective table.
#        Returns an empty dictionary if the database contains no user tables
#        or cannot be accessed.
# 2025-04-15
#--------------------------------------------------------------------
def get_sqlite_schema_readonly(db_path):
    """Function to return a dictionary whose keys are the table names of the SQLite3
    database 'db_path', and whose values are a list with the name of the columns 
    of the respective table.
      
    """
    schema_dict: Dict[str, List[str]] = {}

    # Ensure the file exists before attempting to connect in read-only mode
    # Although connect with mode=ro handles non-existent files, checking
    # first can provide a slightly clearer error sometimes. However,
    # relying on sqlite3's error is more direct.
    # if not os.path.exists(db_path):
    #     raise FileNotFoundError(f"Database file not found at: {db_path}")
    # if not os.path.isfile(db_path):
    #      raise ValueError(f"Path is not a file: {db_path}")

    # Construct the URI for read-only connection. This is the standard way.
    # Using `mode=ro` prevents accidental modification and potentially allows
    # opening files that the user only has read permissions for.
    db_uri = f"file:{db_path}?mode=ro"

    try:
        with sqlite3.connect(db_uri, uri=True) as conn:
            cursor = conn.cursor()

            # Get  table names from the master table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
            tables = cursor.fetchall() # Returns a list of tuples, e.g., [('users',), ('products',)]

            # For each table, get its column names
            for table_tuple in tables:
                table_name = table_tuple[0]

                # PRAGMA table_info() to get column details for the table
                cursor.execute(f'PRAGMA table_info("{table_name}");')
                columns_info_L = cursor.fetchall()

                # columns_info_L is a list of tuples, 
                # where each tuple describes a column:
                # (column_id, column_name, data_type, 
                # not_null_flag, default_value, primary_key_flag)
                # column_name: second element (index 1) is the column_name.
                column_names_L = [col_info[1] for col_info in columns_info_L]

                column_names_L.sort()

                # Add the table and its sorted columns to the dictionary
                schema_dict[table_name] = column_names_L

    except sqlite3.OperationalError as e:
        Err_S = f"[ERROR] Cannot access database '{db_path}' in read-only mode: {e}"
        log_and_print_error(Err_S)
        # Re-raise the exception so the caller knows the operation failed
        raise e

    except Exception as e:
        # Catch all other exceptions
        Err_S = f"[ERROR] Unexpected error occurred: {e}"
        log_and_print_error(Err_S)
        raise e 

    return schema_dict

#--------------------------------------------------------------------
# Opens a SQLite3 database in read-only mode and retrieves the names 
# of all the user tables and the corresponding number of columns
# 
# @param db_path [IN]: File path to the SQLite3 database
#
# @return A dictionary where keys are the names of the tables in the database,
#        and values are the number of columns of the respective table.
#        Returns an empty dictionary if the database contains no user tables
#        or cannot be accessed.
# 2025-04-15
#--------------------------------------------------------------------
def get_sqlite_tables_cols_count(db_path):
    """
    Function to return a dictionary whose keys are the table names of the SQLite3
    database 'db_path', and whose values are the number of columns.      
    """
    schema_count_dict: Dict[str, int] = {}

    # Open the DB in read-only mode (so that no interference with WAL is done)
    db_uri = f"file:{db_path}?mode=ro"

    try:
        with sqlite3.connect(db_uri, uri=True) as conn:
            cursor = conn.cursor()

            # Get  table names from the master table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
            tables = cursor.fetchall() # Returns a list of tuples, e.g., [('users',), ('products',)]

            # For each table, get its column names and then count the number of columns
            for table_tuple in tables:
                table_name = table_tuple[0]

                # PRAGMA table_info() to get column details for the table
                cursor.execute(f'PRAGMA table_info("{table_name}");')
                columns_info_L = cursor.fetchall()

                # columns_info_L is a list of tuples, where each tuple describes a column:
                # (column_id, column_name, data_type, not_null_flag, default_value, primary_key_flag)
                # column_name: second element (index 1) is the column_name.
                num_columns_of_table = len([col_info[1] for col_info in columns_info_L])

                # Add the table and its number of columns to the DICT
                schema_count_dict[table_name] = num_columns_of_table

    except sqlite3.OperationalError as e:
        Err_S = f"[ERROR] Cannot access database '{db_path}' in read-only mode: {e}"
        log_and_print_error(Err_S)
        raise e

    except Exception as e:
        # Catch all other exceptions
        Err_S = f"[ERROR] Unexpected error occurred: {e}"
        log_and_print_error(Err_S)
        raise e 

    return schema_count_dict

#--------------------------------------------------------------------
# Function to compare two dictionaries: current_D and expected_D,
# were expected is the reference dictionary.
# @param current_D [IN] one dictionary to be compared
# @param expected_D [IN] The other dictionary  
# @return Function returns a list of strings, if any difference is found
# Otherwise, it returns a list with the string ["OK"]
# 2025-04-16
#--------------------------------------------------------------------
def compare_dicts_L(current_D, expected_D):
    # Normalize the dictionaries by converting keys to lowercase
    current_copy_D = {k.lower(): v for k, v in current_D.items()}
    expected_copy_D = {k.lower(): v for k, v in expected_D.items()}

    # Find keys that exist in current_D but not in expected_D
    keys_in_current_not_in_expected = set(current_copy_D.keys()) - set(expected_copy_D.keys())

    # Find keys that exist in expected_D but not in current_D
    keys_in_expected_not_in_current = set(expected_copy_D.keys()) - set(current_copy_D.keys())

    # Find keys that exist in both, but whose values are different
    keys_with_different_values = {k for k in set(current_copy_D.keys()) & set(expected_copy_D.keys()) if current_copy_D[k] != expected_copy_D[k]}

    # Generate the report
    report_L = []
    if keys_in_current_not_in_expected:
        report_L.append(f"New keys: {', '.join(keys_in_current_not_in_expected)}")
    if keys_in_expected_not_in_current:
        report_L.append(f"Removed Keys: {', '.join(keys_in_expected_not_in_current)}")
    if keys_with_different_values:
        report_L.append(f"Keys with changed values: {', '.join(keys_with_different_values)}")

    # If no differences are found, return "OK"
    if not report_L:
        return ["OK"]

    return report_L


#--------------------------------------------------------------------
# 2025-06-06
#--------------------------------------------------------------------
def get_file_size(file_path):
    """
    Returns the size of a file in bytes.
    Args:
        file_path (str): The path to the database file.
    Returns:
        int: The size of the file in bytes, or -1 if the file does not exist.
    """
    if os.path.exists(file_path):
        return os.path.getsize(file_path)
    else:
        print(f"Error: Database file not found at '{file_path}'")
        return -1


#====================================================================
# Main
#====================================================================
def main():
    # Create the parser
    parser = argparse.ArgumentParser(description='Print the structure of the CapabilitAcessManager.db passed through the command line and compares it with the CapabilitAcessManager.db of W22H3 [9 tables].')

    parser.add_argument('--path','-p',type=str,required=True,help='Path to SQLite3 database')
    parser.add_argument('--count','-c',action='store_true',help='List tables and their number of columns')
    parser.add_argument('--list','-l',action='store_true',help='List tables and their columns')
    parser.add_argument('--windows','-w',
        choices=['W23H2', 'W24H2', 'w23h2', 'w24h2'], # List of allowed choices
        metavar='WINDOWS_VERSION', # Name to display in help message
        help="Specify the Windows version (e.g., W23H2 or W24H2). Case-insensitive options are also accepted.",
        required=True
    )

    # Parse the arguments
    args = parser.parse_args()

    # Get the DB path
    db_path = args.path
    if not os.path.exists(db_path):
        print(f"[ERROR] Cannot find '{db_path}'", file=sys.stderr)
        sys.exit(1)
    else:
        before_processing_db_file_size = get_file_size(db_path)


    windows_args_upper = args.windows.upper()
    if windows_args_upper == 'W23H2':
        tables_cam_db_cols_count_D = C_TABLES_CAM_DB_COLS_COUNT_W23H2_D
        tables_cam_db_D = C_TABLES_CAM_DB_W23H2_D
    elif windows_args_upper == 'W24H2':
        tables_cam_db_cols_count_D = C_TABLES_CAM_DB_COLS_COUNT_W24H2_D
        tables_cam_db_D = C_TABLES_CAM_DB_W24H2_D

    db_schema_D = {}
    diff_L = []
    if args.count:
        db_schema_D = get_sqlite_tables_cols_count(db_path)
##FIXME:DELETE:
        ## diff_L = compare_dicts_L(db_schema_D,C_TABLES_CAM_DB_COLS_COUNT_W23H2_D)
        diff_L = compare_dicts_L(db_schema_D,tables_cam_db_cols_count_D)

    elif args.list:
        db_schema_D = get_sqlite_schema_readonly(db_path)
##FIXME:DELETE:
        ## diff_L = compare_dicts_L(db_schema_D,C_TABLES_CAM_DB_W23H2_D)
        diff_L = compare_dicts_L(db_schema_D,tables_cam_db_cols_count_D)

    else:
        print(f"[ERROR] Missing option: --count/-c or --list/-l",file=sys.stderr)
        sys.exit(1)


    C_SEP_S = "="*80

    after_processing_db_file_size = get_file_size(db_path)
    print(C_SEP_S)
    print("[INFO] tables/# of columns of SQLite 3 database")
    print(f"Database path: '{args.path}' ({before_processing_db_file_size} to {after_processing_db_file_size} bytes)")
    pprint.pp(db_schema_D)

    num_tables = len(db_schema_D)
    print(f"{num_tables} tables")
    sorted_keys_L = sorted(db_schema_D.keys())
    print(sorted_keys_L)

    print(C_SEP_S)
    pprint.pp(diff_L)
    print(C_SEP_S)


#--------------------------------------------------------------------
# Main
#--------------------------------------------------------------------
if __name__ == '__main__':
    main()

