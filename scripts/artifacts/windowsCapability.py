import os
import xmltodict
import sqlite3
from bs4 import BeautifulSoup
from scripts.artifact_report import ArtifactHtmlReport
from scripts.ilapfuncs import logfunc, tsv, timeline, is_platform_windows, open_sqlite_db_readonly, get_next_unused_name
import json
import datetime
import sys
import pprint
import hashlib
import csv
import codecs
import subprocess
import platform
from typing import Dict, List, Any
import time

# import inspect
# __LINE__ = inspect.currentframe()
# print(__LINE__.f_lineno)
# print(__LINE__.f_lineno)
#====================================================================
# INFO:
# CAM database is located at:
# %PROGRAMADATA%\Microsoft\Windows\CapabilityAccessManager\
# The database name is: "CapabilityAccessManager.db"
#====================================================================
#
# **How to run the module from the command-line**
# -t fs --> filesystem
# -i DIR --> DIR is a directory which must contain a directory called 
# 'CapabilityAccessManager', itself holding the "capabilityAccessManager.db"
# -o DIR --> Where the HTML reports and TSV files are to be created.
# -m windowsCapability --> name of WLEAPP's CAM module(mandatory)
# Example:
# python wleapp.py -t fs -i DirCapabilityDB -o OUT -m windowsCapability
#
# **Running with ZIP**
# -t zip: input is a ZIP file
# -i ArchiveFS.zip: ZIP files holding a directory hierarchy, itself holding
# a directory called CapabilityAccessManager which has a 
# "capabilityAccessManager.db" database.
# Example:
# python wleapp.py -t zip -i ArchiveFS.zip -o R24H2 -m windowsCapability
#
# **AMCACHE functionality: (optional)**
# To have access to the AMcache comparison regarding FileID and ProgramID,
# it is necessary to run Eric Zimmermann's AmcacheParser.exe:
# AmcacheParser.exe -f "C:\Windows\appcompat\Programs\Amcache.hve" --csv  CSV_Destination_DIR
# The CSV file '*unassociatedFileEntries.csv' created should then be 
# specified in the 'wleap-WindowsAccess.json' file as follows:
#  "amcache":{
#    "csv_filename_comment":"CSV file created by E.Zimmerman's AmCache tool with the unassociated entries of the AmCache",
#    "csv_filename":"Amcache_UnassociatedFileEntries.csv"
#  }
# Location of Amcache (W11): C:\Windows\AppCompat\Programs
#
#====================================================================

# How to install (resorting to 'uv')
#
# 1) uv venv CAM
# 2) CAM\Scripts\activate
# 3) uv pip install -r requirements.txt
#

#====================================================================
# Configuration
#====================================================================
# Integer codes used as identifier of the CAM DB version
# It has the same number of tables as W23_H2
C_W23H2      = 1    
# It has the same number of tables as W23H2, but number of columns for certain tables is different
C_W23H2_DIFF = 2    
C_W24H2      = 3
C_W24H2_DIFF = 4
C_UNKNOWN    = 0

#====================================================================
# W23H2
#====================================================================
# Number of tables of CAM DB 
C_NUM_TABLES_CAM_DB_W23H2 = 9   # W11/23H2
# List of tables
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

C_TABLES_CAM_DB_COLS_COUNT_W24H2_D = {'Capabilities': 2,
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
 'NonPackagedGlobalPromptHistory': 6
 }

#====================================================================
# CODE
#====================================================================

#--------------------------------------------------------------------
# Simple string converter of numeric 'db_version'
# @param db_version
# @return String representation of 'db_version'
# 2025-04-17
#--------------------------------------------------------------------
def CAM_version_str(db_version):
    if db_version == C_W23H2:
        return "C_W23H2"

    if db_version == C_W23H2_DIFF:
        return "C_W23H2_DIFF"

    if db_version == C_W24H2:
        return "C_W24H2"

    if db_version == C_W24H2_DIFF:
        return "C_W24H2_DIFF"

    if db_version == C_UNKNOWN:
        return "C_UNKNOWN"

    # Still here? Not good...
    Err_S = f"Non-defined db_version '{db_version}'"
    logfunc(Err_S)
    return Err_S


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
def get_sqlite_tables_cols_count(db_path, debug_flag=False):
    """
    Function to return a dictionary whose keys are the table names of the SQLite3
    database 'db_path', and whose values are the number of columns.      
    """
    schema_count_dict: Dict[str, int] = {}

    try:
         with open_sqlite_db_readonly(db_path) as conn:
            cursor = conn.cursor()

            # Get  table names from the master table
            # cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables_L = cursor.fetchall() # Returns a list of tuples, e.g., [('table1',), ('table2',)]

            # DEBUG
            if debug_flag:
                num_tables = len(tables_L)
                logfunc(f"[DEBUG] List of the {num_tables} tables:{tables_L}")

            # For each table, get its column names
            for table_tuple in tables_L:
                table_name = table_tuple[0]

                # PRAGMA table_info() to get column details for the table
                cursor.execute(f'PRAGMA table_info("{table_name}");')
                columns_info_L = cursor.fetchall()

                # columns_info_L is a list of tuples, where each tuple describes a column:
                # (column_id, column_name, data_type, not_null_flag, default_value, primary_key_flag)
                # column_name: second element (index 1) is the column_name.
                num_columns_of_table = len([col_info[1] for col_info in columns_info_L])

                # DEBUG
                if debug_flag:
                    Debug_S = f"[DEBUG] table '{table_name} has {num_columns_of_table} columns"
                    logfunc(Debug_S)

                # Add the table and its number of columns to the DICT
                schema_count_dict[table_name] = num_columns_of_table

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

    # DEBUG
    if debug_flag:
        Debug_S = f"Schema with # of columns per table for DB '{db_path}'"
        logfunc(Debug_S)
        pprint.pp(schema_count_dict)

    return schema_count_dict


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
    # Dict to return
    schema_dict: Dict[str, List[str]] = {}

    try:
        # Use a 'with' statement to ensure the connection is automatically closed
        with open_sqlite_db_readonly(db_path) as conn:
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

                # columns_info_L is a list of tuples, where each tuple describes a column:
                # (column_id, column_name, data_type, not_null_flag, default_value, primary_key_flag)
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


## #--------------------------------------------------------------------
## # Execute an external EXE and capture its output in a list.
## # 
## # @param exe_path (str): Path to the executable file
## # @param command_options (List[str]): List of command line options
## # @return List where each element is a line of output 
## # from the executable, or None if execution was unsuccessful.
## # 2025-03-31
## # CODE not yet used
## #--------------------------------------------------------------------
## def execute_exe(exe_path, command_options):
##     """execute_exe"""
## 
##     # Check if the executable exists
##     if not os.path.isfile(exe_path):
##         print(f"Error: Executable '{exe_path}' does not exist.")
##         return None
##     
##     # Check if the executable has execute permissions
##     if not os.access(exe_path, os.X_OK):
##         print(f"Error: Executable '{exe_path}' does not have execute permissions.")
##         return None
##     
##     # Prepare the command with arguments
##     command = [exe_path] + command_options
##     
##     try:
##         # Execute the command and capture output
##         process = subprocess.Popen(
##             command,
##             stdout=subprocess.PIPE,
##             stderr=subprocess.STDOUT,  # Redirect stderr to stdout
##             text=True,                # Return text instead of bytes
##             universal_newlines=True   # Handle line endings correctly
##         )
##         
##         # Read the output line by line
##         output_lines = []
##         for line in process.stdout:
##             output_lines.append(line.rstrip())  # Remove trailing newlines
##             
##         # Wait for the process to complete and get return code
##         return_code = process.wait()
##         
##         # If process exited with non-zero return code, it was unsuccessful
##         if return_code != 0:
##             print(f"Error: Process exited with return code {return_code}")
##             return None
##             
##         return output_lines
##         
##     except Exception as e:
##         log_and_print_error(f"Error executing '{exe_path}': {str(e)}")
##         return None
## #--------------------------------------------------------------------

#--------------------------------------------------------------------
# @parameter db_path [IN] path for the main SQLite3 database
# 2025-03-29
#--------------------------------------------------------------------
def lookup_and_report_fileID_in_CSV(db_path, csv_file_path, csv_db_table_name, report_ID, report_folder, debug_flag=False):
    """Function to lookup fileIDs in the csv_file_path file. 
       This CSV file is obtained through Eric Zimmermann's "AmCacheParser.exe" corresponding 
       to the "*Amcache_UnassociatedFileEntries.csv". 
       1) The function creates a single table SQLite 3 database from the CSV file.
       2) The function runs a SQL query on the NonPackagedIdentityRelationship table
       3) The function lookups in the (temporary) created table every FileID returned 
       from NonPackagedIdentityRelationship. Whenever a match is found, a SQL query is performed
       in the single table to extract the whole AmCache data of the matching FileID.
    """
    #--------------------------------------------
    # Process CSV file, inserting its data to a 
    # SQLite3 database
    #--------------------------------------------
    csv_data_L, csv_info_D = process_csv_to_list_of_tuples(csv_file_path, debug_flag=False)
    if csv_data_L is None:
        Warn_S = f"[WARNING] No data extracted from CSV '{csv_file_path}'"
        logfunc(Warn_S)
        return None

    # Still here? Good
    if debug_flag:
        logfunc(csv_info_D["info_S"])

    csv_db_path = "amcache_csv_(temp).db"
    # Delete csv_db_path if it exists from a previous execution
    delete_file_if_exists(csv_db_path)

    #--------------------------------------------
    # Create 'csv_db_path' SQLite 3 database, and
    # then with the content 
    # of csv_data_L, creates the table 
    # 'csv_db_table_name' (in 'csv_db_path')
    #--------------------------------------------
    create_sqlite_db_from_list(csv_data_L, csv_db_path, csv_db_table_name)

    # Run NonPackagedIdentityRelationship SQL query
    sql_S = """SELECT datetime((LastObservedTime/10000000)-11644473600,'unixepoch','localtime') AS Last_observed_time,
BinaryFullPaths.StringValue as Bin_full_Path,
ProgramIDs.StringValue as Program_hash,
NonPackagedIdentityRelationship.ProgramID,
FileIDs.StringValue as File_hash,
NonPackagedIdentityRelationship.FileID
FROM NonPackagedIdentityRelationship
INNER JOIN BinaryFullPaths
ON NonPackagedIdentityRelationship.BinaryFullPath = BinaryFullPaths.ID 
INNER JOIN ProgramIDs
ON NonPackagedIdentityRelationship.ProgramID = ProgramIDs.ID
INNER JOIN FileIDs
ON NonPackagedIdentityRelationship.FileID = FileIDs.ID"""

    founds_L = []
    FILE_ID_idx = 4 # Index of FileID in record_L
    NonPackagedIdentityRelationship_L = execute_sql_query_in_database(db_path, sql_S)
    for record_L in NonPackagedIdentityRelationship_L:
        file_ID = record_L[FILE_ID_idx]
        # file_ID is something like this xxxx|DATA|yyyy|zzzz ("|" is just 
        # for visual separation in this comment).
        # We want to remove xxxx + yyyy + zzzz
        # After extract_first_fours, file_ID_filtered is DATA.
        removed_text, file_ID_filtered = extract_first_fours(file_ID)
        if file_ID_filtered is None:
            # Hum, empty file_ID_filtered :-/
            # Try next file_ID
            continue

        #  if removed_text is not None:
        #      logfunc(f"[DEBUG] REMOVED:{removed_text} (file_ID={file_ID}")

        # Lookup 'file_ID_filtered' in csv_db_path
        sql_fileID_S = f"""SELECT SHA1, FullPath, Name
FROM {csv_db_table_name}
WHERE SHA1 like '{file_ID_filtered}'"""
        results_L = execute_sql_query_in_database(csv_db_path, sql_fileID_S)
        if (results_L is not None) and (len(results_L)>0):
            # DEBUG
            if debug_flag:
                logfunc(f"[INFO] file_ID '{file_ID_filtered}' FOUND in table '{csv_db_table_name}'")

            founds_L.append(file_ID_filtered)
        else:
            pass
            # print(f"[INFO] file_ID '{file_ID_filtered}' NOT found in CSV table")

    if len(founds_L) == 0:
        # No file_ID found. Bail out
        Info_S = f"[INFO] No file_ID found in {csv_file_path}"
        logfunc(Info_S)
        return []

    # INFO
    if debug_flag:
        Info_S = f"[INFO] File_ID:{len(founds_L)} IDs found in AmCache CSV file;\n{founds_L}" 
        logfunc(Info_S)

    #--------------------------------------------
    # Still here? Ok, at least one filter ID 
    # was found. Let's create a report.
    #--------------------------------------------
    filter_IDs = ",".join(f"'{item}'" for item in founds_L)
    Id_S = f'{report_ID}'
    name_S = f'{Id_S}_[IDs_in_amcache]'

    # Format the message to be inserted in the HTML report
    num_CSV_entries = len(csv_data_L)
    msg_in_report = f"[]; AmCache CSV='{csv_file_path}' ({num_CSV_entries} entries)"
    
    # dates_filters_S = "" # No dates filter
    tsvname = f'{Id_S}_CAM_ID_in_amcache'

    sql_fileID_S = f"""SELECT FileKeyLastWriteTimestamp as LastWrite, FullPath, SHA1, 
LinkDate, Size, Name, ProgramId, version, ProductVersion
FROM {csv_db_table_name}
WHERE SHA1 in ({filter_IDs})"""

    try:
        # Connect to the (temporary) database 
        # (creates it if it doesn't exist)
        conn = sqlite3.connect(csv_db_path)
        cursor = conn.cursor()

        headers_L = ("LastWrite","FullPath","SHA1","LinkDate",
                     "Size","Name","ProgramId","Version","ProductVersion")
        create_report_and_tsv(report_folder, cursor, name_S, sql_fileID_S, 
                              headers_L, tsvname, csv_db_path, msg_in_report)

    except sqlite3.Error as e:
        Err_S = f"ERROR: sqlite3 '{e}'"
        log_and_print_error(Err_S)
    finally:
        if conn:
            conn.close()

    # Delete temporary database if it exists
    delete_file_if_exists(csv_db_path)

    return founds_L

#--------------------------------------------------------------------
# Extracts the first four characters and the last two groups of 
# four characters from a string and return them in list with three elems
# (one elm = one group of four characters)
# @param text [IN] text to process
# @return  A list containing the first four characters, the 
#           second-to-last four characters, and the last four 
#           characters. Returns an empty list if the input is 
#          invalid or the string is too short.
#          Also returns the text without the three blocks of four
#          Characters
# 2025-03-29
#--------------------------------------------------------------------
def extract_first_and_last_two_fours(text):
    # We only deal with strings
    if not isinstance(text, str):
        return []

    # 4 + 4 + 4 = 12...
    if len(text) < 12:  
        return [] 

    first_four          = text[:4]
    second_to_last_four = text[-8:-4]
    last_four           = text[-4:]
    remaining_text      = text[4:-8]

    return [first_four, second_to_last_four, last_four], remaining_text

#--------------------------------------------------------------------
# 2025-03-29
#--------------------------------------------------------------------
def extract_first_fours(text):
    # We only deal with strings
    if not isinstance(text, str):
        return None, None

    if len(text) < 4:  
        return [] 

    first_four          = text[:4]
    remaining_text      = text[4:]

    return first_four, remaining_text

#--------------------------------------------------------------------
# Process a CSV file, creating a list of tuple. Each tuple is a 
# row of the CSV file, with the first tuple of the list being 
# the CSV header.
# The function uses the number of fields in the CSV header (i.e., 1st row) 
# as the reference, so that any row of the CSV that has a different
# number of fields in not included in the result_L list. 
# @param csv_file_path [IN] CSV file to process
# @param separator     [IN] CSV separator. Default is "," 
# @param debug_flag    [IN] True to print a debug message
# @return 
# - List of tuples corresponding to the accepted rows of the CSV file
# - dictionary with the keys: 
# "num_row_OK","num_row_rejected", and "num_fields"
# 2025-03-28
#--------------------------------------------------------------------
def process_csv_to_list_of_tuples(csv_file_path, separator=',', debug_flag=False):
    result_L         = []
    rejected_L       = []
    num_row_OK       = 0
    num_row_rejected = 0 
    num_fields       = 0
    header_row_L     = []

    with open(csv_file_path, 'r',  encoding='utf-8') as file:
        is_first_line = True
        for line in file:
            if is_first_line:
                # Header row?
                is_first_line = False
                stripped_line = line.strip()
                header_row_L = tuple(stripped_line.split(separator))
                # Number of fields is set by header_row_L
                num_fields = len(header_row_L)
                # header row is the first tuple of result_L
                result_L.append(header_row_L)
                continue

            # Strip any leading/trailing whitespace (including newlines)
            stripped_line = line.strip()
            # Split the line based on the provided separator
            row = tuple(stripped_line.split(separator))
            if len(row) == num_fields:
                # Append the tuple to the result list
                result_L.append(row)
                num_row_OK += 1
            else:
                # Bar row: too few or too many fields
                rejected_L.append(row)
                num_row_rejected += 1

    # print("Rejected rows:")
    # pprint.pprint(rejected_L)
    info_S = f"# of fields: {num_fields}\nRows: OK={num_row_OK}; Rejected={num_row_rejected}"
    results_D = {"num_row_OK":num_row_OK,
                 "num_row_rejected":num_row_rejected, 
                 "num_fields":num_fields,
                 "info_S":info_S}

    if debug_flag:
        logfunc(f"CSV: {csv_file_path}; {info_S}")

    return result_L, results_D


#--------------------------------------------------------------------
# Creates a SQLite3 database named 'db_path' with a table named 
# 'table_name' and filled with the data of the list 'data_L'.
# The first row of 'data_L' is used for the name of the columns.
# All fields of the table are TEXT.
# @param data_L     [IN] list of tuples with the header (1st row) and data
# @param db_path    [IN] path of the SQLite3 file to create/update
# @param table_name [IN] name of the table to create
# @return   None
# 2025-03-29
#--------------------------------------------------------------------
def create_sqlite_db_from_list(data_L, db_path, table_name, debug_flag=False):
    """
    Creates an SQLite 3 database from a list of tuples.
    Args:
        data_L (list): A list of tuples, where the first tuple represents the 
                       header row and subsequent tuples represent the data rows.
        db_path (str): The path to the SQLite database file to create.
        table_name (str): The name of the table to create in the database.
    """

    try:
        # Connect to the database (creates it if it doesn't exist)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Extract header (column names) from the first tuple
        header_T = data_L[0]

        num_fields = len(header_T)

        #----------------------------------------
        # Create the table with TEXT fields for 
        # all columns
        #----------------------------------------

        # Define the columns based on the first row (header_T)
        column_definitions = ", ".join(f"{col} TEXT" for col in header_T) 
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions})"
        cursor.execute(create_table_sql)

        #----------------------------------------
        # Insert data into the table (skip 
        # the header row)
        #----------------------------------------

        # Create placeholders for parameterized query
        placeholders = ", ".join("?" for _ in header_T)
        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

        for row in data_L[1:]: 
            cursor.execute(insert_sql, row)

        # Commit the changes and close the connection
        conn.commit()

        # DEBUG
        if debug_flag:
            Debug_S = f"[DEBUG] Database '{db_path}' / table '{table_name}': OK"
            logfunc(Debug_S)

    except sqlite3.Error as e:
        Err_S = f"ERROR: sqlite3 '{e}'"
        log_and_print_error(Err_S)
    finally:
        if conn:
            conn.close()

        return None

#--------------------------------------------------------------------
# Executes a SQL query on a SQLite database and returns the results 
# as a list of tuples.
# @param db_path [IN] Path to the SQLite database file
# @param sql_s   [IN] string with SQL query
# @return
#  A list of tuples, where each tuple represents a row from the result set.
#  Returns an empty list if the query returns no results or if an error occurs.
#  Returns None if db_path or query are None or empty strings.
#  # Example queries
#  results = execute_sql_query(db_file, "SELECT * FROM users")
# Positional parameters
#  results = execute_sql_query(db_file, "SELECT name FROM users WHERE age > ?", (30,)) 
# Named parameters
#  results = execute_sql_query(db_file, "SELECT name FROM users WHERE age = :age", {"age": 25}) 
#  results = execute_sql_query(db_file, "SELECT * FROM users WHERE name LIKE ?", ('B%',))
# 2025-03-27
#--------------------------------------------------------------------
def execute_sql_query_in_database(db_path, sql_s, params=None):
    "execute_sql_query"

    if not db_path or not sql_s:
        return None

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        if params:
            cursor.execute(sql_s, params)
        else:
            cursor.execute(sql_s)

        results_L = cursor.fetchall()

        conn.close()
        return results_L

    except sqlite3.Error as e:
        Err_S = f"SQLite error: {e}"
        log_and_print_error(Err_S)
    return None  # Return an empty list in case of error

#--------------------------------------------------------------------
# Function that runs a SQL query and creates an HTML report and
# a tsv file.
# 'report_folder' [IN]: folder where to write the report
# 'cursor_db'     [IN]: Cursor to the SQLite 3 database 
# 'name_S'        [IN]: name of the report to be created
# 'sql_S'         [IN]: SQL code to execute
# 'headers_L      [IN]: list of headers
# 'tsvname'       [IN]: name of the TSV (Tab Separated Value) file
# 'file_found'    [IN]: file being processed
# 2025-03-04
#--------------------------------------------------------------------
def create_report_and_tsv(report_folder, cursor_db, name_S, sql_S, 
     headers_L, tsvname, file_found, dates_filter_str=None, debug_flag=False):
    """Execute the SQL query 'sql_S' over the SQLite3 cursor."""

    cursor_db.execute(sql_S)

    all_rows = cursor_db.fetchall()
    usageentries = len(all_rows)

    if debug_flag:
        Debug_S = f"[DEBUG] Report '[{name_S}]' has {usageentries} entries"
        logfunc(Debug_S)

    if usageentries == 0:
        # No data: create a list with the word "No data" in each of its fields

        no_data_S = "(empty)"
        all_rows = []
        headers_copy_L = list(headers_L[:])
        no_data_L = []
        for i in range(len(headers_copy_L)):
            no_data_L.append(no_data_S)
        all_rows.append(tuple(no_data_L))


    if usageentries >= 0:
        report = ArtifactHtmlReport(name_S, dates_filter_S = dates_filter_str)
        report.start_artifact_report(report_folder, name_S)
        report.add_script()

        data_list = []

        data_headers = headers_L
        for rows in all_rows:
            try:
                payload = rows[0]
                # Note: payload needs to be a string,
                # so for queries that return an INT as first field,
                # we convert to a string.
                # 2025-05-16 
                if isinstance(payload, int):
                    payload = str(payload)
                    # DEBUG
                    Debug_S = f"[INFO] Converted 'int' to 'str': '{payload}'"
                    print_banner(Debug_S)

                payload_text = BeautifulSoup(payload, 'html.parser').text

                data_row_L = list(rows[1:])
                data_row_L.insert(0,payload_text)
                data_list.append(data_row_L)
            except Exception as e:
                Err_S = f"ERROR:['{name_S}']: {e}"
                log_and_print_error(Err_S)
                raise

        report.write_artifact_data_table(data_headers, data_list, file_found)
        report.end_artifact_report()

        if usageentries > 0 and tsvname and len(tsvname) > 0:
            # Create TSV file (only if usageentries has data)
            tsv(report_folder, data_headers, data_list, tsvname)
        else:
            logfunc(f"[INFO] No results for '{name_S}' (no TSV was created)")


#--------------------------------------------------------------------
# Convert a date string in YYYY-MM-DD format to Microsoft Filetime64.
# Microsoft Filetime represents time as 100-nanosecond intervals since 
# January 1, 1601 UTC.
# Args:       
#   date_string (str): Date in format 'YYYY-MM-DD'
# Returns:
#        int: The date as a Microsoft Filetime64 value
# 2025-03-18
#--------------------------------------------------------------------
def date_to_filetime(date_string):
    # Parse the input date
    try:
        dt = datetime.datetime.strptime(date_string, '%Y-%m-%d')
    except ValueError as e:
        Err_S = f"Invalid date '{date_string}' {e} (Expected format is YYYY-MM-DD)"
        log_and_print_error(Err_S)
        
        return None
    
    # Windows epoch is January 1, 1601
    win_epoch = datetime.datetime(1601, 1, 1)
    
    # Calculate the time difference in seconds
    delta_seconds = (dt - win_epoch).total_seconds()
    
    # Convert to 100-nanosecond intervals (10,000,000 per second)
    filetime = int(delta_seconds * 10_000_000)
    
    return filetime

#--------------------------------------------------------------------
# Returns a string with SQL code based on 'start_date_ft64' and 
# 'end_date_ft64' to effectively implement a SQL filter.
#
# Parameters:
#   start_date_ft64 (float): The start date in a specific format (likely a timestamp).
#   end_date_ft64 (float): The end date in a specific format (likely a timestamp).
#   field_name_S (str): The name of the field to be used in the SQL query.
#
# Returns:
#   str: A SQL WHERE clause string based on the provided dates, 
#        or None if no filtering is needed.
# 2025-03-19
#--------------------------------------------------------------------
def start_date_and_end_date_to_sql(start_date_ft64, end_date_ft64, field_name_S):

    # Check if both start_date_ft64 and end_date_ft64 are None
    if (start_date_ft64 is None) and (end_date_ft64 is None):
        # No data filtering needed
        return None

    # Check if only start_date_ft64 is provided
    if (start_date_ft64 is not None) and (end_date_ft64 is None):
        # Create SQL WHERE clause for start date filtering
        return f"WHERE ({field_name_S} >= {start_date_ft64})"

    # Check if only end_date_ft64 is provided
    if (start_date_ft64 is None) and (end_date_ft64 is not None):
        # Create SQL WHERE clause for end date filtering
        return f"WHERE ({field_name_S} <= {end_date_ft64})"

    # Check if both start_date_ft64 and end_date_ft64 are provided
    if (start_date_ft64 is not None) and (end_date_ft64 is not None):
        # Create SQL WHERE clause for both start and end date filtering
        return f"WHERE (({field_name_S} >= {start_date_ft64}) and ({field_name_S} <= {end_date_ft64}))"

    # Still here? No filtering
    return None


#--------------------------------------------------------------------
# Count the number of records for all tables in a SQLite3 database 
# in read-only mode.
# The function creates a read-only connection to db_path, queries 
# the DB, and then close the connection.
# @returns String with table counts on success, None otherwise.
# 2025-03-26
#--------------------------------------------------------------------
def count_records_per_table_S(db_path):
    # Check if the database file exists
    if not os.path.exists(db_path):
        print()
        Err_S = f"Error: Database file '{db_path}' does not exist." 
        log_and_print_error(Err_S)
        return None

    try:
        # Get the size of the DB database file? 
        db_size_bytes = os.path.getsize(db_path)

        # Open the database in read-only mode
        conn = open_sqlite_db_readonly(db_path)
        cursor = conn.cursor()
        
        # Get list of all tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        # Dictionary to store record counts
        record_counts_D = {}
        
        # Count records for each table
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            record_counts_D[table_name] = count

        # New dict sorted by table names
        sorted_record_counts_D = dict(sorted(record_counts_D.items()))

        record_counts_S = ""
        for table, count in sorted_record_counts_D.items():
            record_counts_S = record_counts_S + f"{table}: {count} records\n"
        record_counts_S = record_counts_S + f"DB size:{db_size_bytes} bytes"
        return record_counts_S

    except sqlite3.Error as e:
        Err_S = f"ERROR: count_records_per_table_S() - '{e}'"
        log_and_print_error(Err_S)
        return None
    
    finally:
        # Ensure the connection is closed
        if 'conn' in locals():
            conn.close()

#--------------------------------------------------------------------
# Compare two dictionaries comprehensively.
# @param dict1 [IN] One of the dictionaries to compare
# @param dict2 [IN] The other dictionary
# @param output_L [IN][OUT] list where output should go (with append)
# @returns: True if dictionaries are identical, False otherwise
# 2025-03-26
#--------------------------------------------------------------------
def compare_dictionaries(dict1, dict2, output_L):
    # Check if the dictionaries have the same keys
    keys1 = set(dict1.keys())
    keys2 = set(dict2.keys())
    
    # Find unique keys in each dictionary
    unique_keys_dict1 = keys1 - keys2
    unique_keys_dict2 = keys2 - keys1
    common_keys = keys1 & keys2
    
    # Print comparison results
    output_L.append("Dictionary Comparison Report:")
    output_L.append("=" * 40)
    # Report on unique keys
    if unique_keys_dict1:
        output_L.append("Keys only in first dictionary:")
        for key in unique_keys_dict1:
            output_L.append(f"  - {key}: {dict1[key]}")
    
    if unique_keys_dict2:
        output_L.append("Keys only in second dictionary:")
        for key in unique_keys_dict2:
            output_L.append(f"  - {key}: {dict2[key]}")
    
    # Compare common keys
    output_L.append("\nCommon Keys Comparison:")
    are_identical = True
    for key in common_keys:
        val1 = dict1[key]
        val2 = dict2[key]
        
        if val1 == val2:
            output_L.append(f"  - {key}: EQUAL")
            output_L.append(f"    Value: {val1}")
        else:
            are_identical = False
            output_L.append(f"  - {key}: DIFFERENT")
            output_L.append(f"    First dictionary:  {val1}")
            output_L.append(f"    Second dictionary: {val2}")
    
    # Overall comparison summary
    output_L.append("\nOverall Comparison:")
    if not common_keys:
        output_L.append("  No common keys found.")
    elif are_identical and not unique_keys_dict1 and not unique_keys_dict2:
        output_L.append("  ✓ Dictionaries are IDENTICAL")
        return True
    else:
        output_L.append("  ✗ Dictionaries are NOT identical")
        return False

#--------------------------------------------------------------------
# Function to compare two dictionaries: current_D and expected_D,
# were expected is the reference dictionary.
# @param current_D [IN] one dictionary to be compared
# @param expected_D [IN] The other dictionary  
# @return Function returns a list of strings, if any difference is found
# Otherwise, it returns an empty list
# 2025-04-16
#--------------------------------------------------------------------
def compare_dicts_L(current_D, expected_D):
    # Convert keys to lowercase
    current_copy_D = {k.lower(): v for k, v in current_D.items()}
    expected_copy_D = {k.lower(): v for k, v in expected_D.items()}

    # Which keys exist in current_D but not in expected_D? 
    keys_in_current_not_in_expected = set(current_copy_D.keys()) - set(expected_copy_D.keys())

    # Which keys exist in expected_D but not in current_D
    keys_in_expected_not_in_current = set(expected_copy_D.keys()) - set(current_copy_D.keys())

    # Keys that exist in both, but whose values are different
    keys_with_different_values = {k for k in set(current_copy_D.keys()) & set(expected_copy_D.keys()) if current_copy_D[k] != expected_copy_D[k]}

    # Generate the report
    report_L = []
    if keys_in_current_not_in_expected:
        report_L.append(f"New keys: {', '.join(keys_in_current_not_in_expected)}")
    if keys_in_expected_not_in_current:
        report_L.append(f"Removed Keys: {', '.join(keys_in_expected_not_in_current)}")
    if keys_with_different_values:
        report_L.append(f"Keys with changed values: {', '.join(keys_with_different_values)}")

    # If no differences are found, return an empty list
    return report_L
    
#--------------------------------------------------------------------
# Compute a message digest for each table of a SQLite3 database, 
# returning a string with this info.
# The function can be used to detect changes in a database.
# @return dictionary with message digest of tables (one entry per table) 
#         if ok, None on error.
# 2025-03-26
#--------------------------------------------------------------------
def compute_table_digest(db_path, hash_algorithm='sha256'):
    # Validate hash algorithm
    try:
        hash_func = getattr(hashlib, hash_algorithm)
    except AttributeError:
        Err_S = f"[ERROR] Unsupported hash algorithm: {hash_algorithm}"
        log_and_print_error(Err_S)
        raise ValueError(Err_S)
    
    try:
        # Connect to the SQLite database in read-only mode
        conn = open_sqlite_db_readonly(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get list of all tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        # Dictionary to store table digests
        table_digests = {}
        
        # Compute digest for each table
        for table in tables:
            table_name = table[0]
            
            # Create a hash object
            hasher = hash_func()
            
            # Fetch all rows from the table
            cursor.execute(f"SELECT * FROM {table_name} ORDER BY rowid;")
            
            # Update hash with each row's content
            for row in cursor.fetchall():
                # Convert row to a string representation
                row_content = str(tuple(row)).encode('utf-8')
                hasher.update(row_content)
            
            # Store the final digest
            table_digests[table_name] = hasher.hexdigest()
        
        # Sort by table name
        sorted_D = dict(sorted(table_digests.items()))

        # Close the connection to the DB
        conn.close()

        return sorted_D

    except sqlite3.Error as e:
        Err_S = f"[ERROR] SQLite error occurred: {e}"
        log_and_print_error(Err_S)
        return None

    except Exception as e:
        Err_S = f"[ERROR] Error occurred: {e}"
        log_and_print_error(Err_S)
        return None
    finally:
        # Ensure the connection is closed
        if 'conn' in locals():
            conn.close()

#--------------------------------------------------------------------
# Attempts to synchronize a SQLite database with its Write-Ahead 
# Logging (WAL) file.
#
# Parameters:
#   db_path (str): The path to the SQLite database file.
#   output_debug_L (list, optional): A list to store debugging information. 
#                                    Defaults to None.
# Returns:
#   bool: True if the synchronization process completes successfully, False otherwise.
# 2025-03-26
#--------------------------------------------------------------------
def sync_WAL_with_DB(db_path, output_debug_L=None):
    wal_path = f"{db_path}-wal"
    if check_file_exists_and_not_empty(wal_path):
        #----------------------------------------
        # Let's try to apply the WAL file to 
        # the DB
        #----------------------------------------
        # List used to save debugging info
        if output_debug_L is not None:
            sep_S = get_sep()
            # We...
            # - i) Display the # of records per table and the size of the DB file 
            # - ii) Compute the sha256 of each table for comparison after applying WAL
            records_per_table_S = count_records_per_table_S(db_path)
            output_debug_L.append(sep_S)
            output_debug_L.append("[INFO] Before 'checkpoint'")
            if records_per_table_S is not None:
                output_debug_L.append(records_per_table_S)

            # Compute digest of tables content
            msg_digest_per_table_before_D = compute_table_digest(db_path, 'sha256')
            output_debug_L.append(sep_S)

        # Ok, there is a non-zero WAL file
        db = open_sqlite_db(db_path)
        cursor = db.cursor()

        # Check WAL mode and database status
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA integrity_check")
        integrity_result = cursor.fetchone()[0]


        if integrity_result == 'ok':
            if output_debug_L is not None:
                output_debug_L.append("[INFO] Database integrity check passed.")

            # Checkpoint the WAL file
            cursor.execute("PRAGMA wal_checkpoint(RESTART);") 
            output_debug_L.append("[INFO] WAL file checkpointed successfully.")

        else:
            Err_S = "[INFO] Integrity check failed: {integrity_result}"
            logfunc(Err_S)
            output_debug_L.append(Err_S)
       
        db.commit()
        db.close()

        if output_debug_L is not None:
            # We...
            # - i) Display the # of records per table and the size of the DB file 
            # - ii) Compute the message digest sha256 of each table after applying WAL
            # - iii) We compare message digest table per table
            records_per_table_S = count_records_per_table_S(db_path)
            sep_S = get_sep()
            output_debug_L.append(sep_S)
            output_debug_L.append("[INFO] after 'checkpoint'")
            if records_per_table_S is not None:
                output_debug_L.append(records_per_table_S)

            # Compute digest of tables content
            msg_digest_per_table_after_D = compute_table_digest(db_path, 'sha256')

            if (msg_digest_per_table_before_D is not None) and\
                    (msg_digest_per_table_after_D is not None):
                compare_dictionaries(msg_digest_per_table_before_D, msg_digest_per_table_after_D, output_debug_L)
            output_debug_L.append(sep_S)

    # DEBUG
    if output_debug_L is not None:
        logfunc("DEBUG - 'output_debug_L'")
        pprint.pp(output_debug_L)

    return True

#--------------------------------------------------------------------
# 2025-04-15
#--------------------------------------------------------------------
def infere_cam_db_version(db_path):
    """Attempt to determine whether we support/recognize database 'db_path'"""
    # Get a dictionary with the tables of 'db_path' and the column count
    # of each of the tables. 
    # Each keys of the dictionary is a table name of the database,
    # whereas the value is the number of columns
    schema_db_tables_cols_count_D = get_sqlite_tables_cols_count(db_path)

    num_tables = len(schema_db_tables_cols_count_D)
    if num_tables == 0:
        # Something is wrong
        Err_S = f"[ERROR] Error processing database '{db_path}'"
        logfunc(Err_S)
        return -1


    ret_code = C_UNKNOWN
    Info_S = ""
    if num_tables == C_NUM_TABLES_CAM_DB_W23H2:
        # The number of tables indicates this might be W23H2. 
        # Let's check if schema_db_tables_cols_count_D has
        # the same content as C_TABLES_CAM_DB_COLS_COUNT_W23H2_D
        compare_L = compare_dicts_L(schema_db_tables_cols_count_D, C_TABLES_CAM_DB_COLS_COUNT_W23H2_D)
        if len(compare_L) != 0:
            ret_code = C_W23H2_DIFF
            # Hum -- changes were detected at the column count level
            Info_S = f"""[WARNING] Column count change detected for {C_CAM_DB}:
- {num_tables} tables (compatible with 'W23H2')
- changes: {compare_L}"""
        else:
            ret_code = C_W23H2
            Info_S = f"[INFO] W23H2 schema ({num_tables} tables)"
    elif num_tables == C_NUM_TABLES_CAM_DB_W24H2:
        # The number of tables indicates this might be W24H2. 
        # Let's check if schema_db_tables_cols_count_D has
        # the same content as C_TABLES_CAM_DB_COLS_COUNT_W24H2_D
        compare_L = compare_dicts_L(schema_db_tables_cols_count_D, C_TABLES_CAM_DB_COLS_COUNT_W24H2_D)
        if len(compare_L) != 0:
            ret_code = C_W24H2_DIFF
            # Hum -- changes were detected at the column count level
            Info_S = f"""[WARNING] Column count change detected for {db_path}:
- {num_tables} tables (compatible with 'W24H2')
- changes: {compare_L}"""
        else:
            ret_code = C_W24H2
            Info_S = f"[INFO] W24H2 schema ({num_tables} tables)"
    else:
        # num_tables is neither C_TABLES_CAM_DB_COLS_COUNT_W23H2_D 
        # or C_TABLES_CAM_DB_COLS_COUNT_W24H2_D 
        # WARN!
        # Get a dictionary with the table names (keys) and the name 
        # of their columns (values)
        schema_db_tables_D = get_sqlite_schema_readonly(db_path)
        Info_S = f"""[WARNING] Unexpected number of tables {num_tables} for {db_path}.
Expecting {C_NUM_TABLES_CAM_DB_W23H2} tables for W23H2 or {C_NUM_TABLES_CAM_DB_W24H2} for W24H2.
Current DB: {schema_db_tables_D}
"""
    if len(Info_S):
        logfunc(Info_S)
        
    return ret_code

#====================================================================
# SQL queries
#====================================================================

#--------------------------------------------------------------------
# Returns the SQL for querying 'PackagedUsageHistory'
# @param db_version [IN] 
# @param query_name [IN] 
# @return SQL_query, list with columns names
# (Empty string and empty list if db_version is unknown)
# not supported
# 2025-04-16
# Query "A"
#--------------------------------------------------------------------
def get_SQL_packagedUsageHistory(db_version, query_name):
    """Returns the core SQL code for querying 'PackageUsageHistory'"""
    # NOTE: there are some overlap in the two SQL queries, and so we 
    # could have a common part to be merged with the specific
    # part of each query. Having two separate queries in the function
    # allows for an easy test (just copy-paste into the SQL engine)

    if db_version in (C_W23H2, C_W23H2_DIFF):
        sql_S = """SELECT CASE WHEN LastUsedTimeStart = 0 THEN 'N.A.'
            ELSE datetime( (LastUsedTimeStart / 10000000) - 11644473600, 'unixepoch', 'localtime') 
        END AS Last_used_start,
datetime( (LastUsedTimeStop / 10000000) - 11644473600, 'unixepoch', 'localtime') AS Last_used_stop, 
CASE WHEN PackagedUsageHistory.AccessBlocked = 0 THEN 'Access OK'
ELSE 'Blocked'
END AS Access,
Capabilities.StringValue as Capability,
PackageFamilyNames.StringValue as PackageName,Users.StringValue,PackagedUsageHistory.ID
FROM PackagedUsageHistory
INNER JOIN Users
on PackagedUsageHistory.userSid = Users.ID
INNER JOIN Capabilities
on PackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN PackageFamilyNames
on PackagedUsageHistory.PackageFamilyName = PackageFamilyNames.ID
"""
        headers_L = ['Last_used_start','Last_used_stop','AccessBlocked','Capability','PackageName','UserSID','ID']

    elif db_version in (C_W24H2, C_W24H2_DIFF):
        sql_S = """
SELECT CASE WHEN LastUsedTimeStart = 0 THEN 'N.A.'
            ELSE datetime( (LastUsedTimeStart / 10000000) - 11644473600, 'unixepoch', 'localtime') 
        END AS Last_used_start,
datetime( (LastUsedTimeStop / 10000000) - 11644473600, 'unixepoch', 'localtime') AS Last_used_stop, 
CASE WHEN PackagedUsageHistory.AccessBlocked = 0 THEN 'Access OK'
ELSE 'Blocked'
END AS Access,
Capabilities.StringValue as Capability,
PackageFamilyNames.StringValue as PackageName,
AppNames.StringValue,
Users.StringValue,
PackagedUsageHistory.Label,
PackagedUsageHistory.ID
FROM PackagedUsageHistory
INNER JOIN Users
on PackagedUsageHistory.userSid = Users.ID
INNER JOIN Capabilities
on PackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN PackageFamilyNames
on PackagedUsageHistory.PackageFamilyName = PackageFamilyNames.ID
INNER JOIN AppNames
On PackagedUsageHistory.AppName = AppNames.ID 
"""
        headers_L=['Last_used_start','Last_used_stop','AccessBlocked','Capability','PackageName','AppName','UserSID','Label','ID']
    else:
        Error_S = f"[ERROR][Query '{query_name}'] Non-supported version for '{db_version}'"
        logfunc(Error_S)

        sql_S = ""
        headers_L = []

    return sql_S, headers_L


#--------------------------------------------------------------------
# Returns the SQL for querying 'PackagedUsageHistory First/Last'
# @param db_version [IN] 
# @return SQL_query, list with columns names
# (Empty string and empty list if db_version is unknown)
# not supported
# 2025-04-16
# Query "X1"
#--------------------------------------------------------------------
def get_SQL_packagedUsageHistory_FirstLast(db_version, query_name):
    """Returns the core SQL code for querying 'PackageUsageHistory' First/Last"""
    # NOTE: there are some overlap in the two SQL queries, and so we 
    # could have a common part to be merged with the specific
    # part of each query. Having two separate queries in the function
    # allows for an easy test (just copy-paste into the SQL engine)

    if db_version in (C_W23H2, C_W23H2_DIFF):
        sql_S = """SELECT MIN(datetime((LastUsedTimeStop/10000000)-11644473600,'unixepoch','localtime')) AS Last_used_stop_min, 
MAX(datetime( (LastUsedTimeStop / 10000000) - 11644473600, 'unixepoch', 'localtime')) AS Last_used_stop_max, 
PackagedUsageHistory.AccessBlocked, Capabilities.StringValue as Capability,
PackageFamilyNames.StringValue as PackageName,
Users.StringValue,
PackagedUsageHistory.ID
FROM PackagedUsageHistory
INNER JOIN Users
on PackagedUsageHistory.userSid = Users.ID
INNER JOIN Capabilities
on PackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN PackageFamilyNames
on PackagedUsageHistory.PackageFamilyName = PackageFamilyNames.ID""" 
        headers_L = ('Last_used_start','Last_used_stop','AccessBlocked','Capability','PackageName','User SID','ID')

    elif db_version in (C_W24H2, C_W24H2_DIFF):
        sql_S = """SELECT MIN(datetime((LastUsedTimeStop/10000000)-11644473600,'unixepoch','localtime')) AS Last_used_stop_min, 
MAX(datetime( (LastUsedTimeStop / 10000000) - 11644473600, 'unixepoch', 'localtime')) AS Last_used_stop_max, 
PackagedUsageHistory.AccessBlocked,
Capabilities.StringValue as Capability,
PackageFamilyNames.StringValue as PackageName,
AppNames.StringValue,
Users.StringValue,PackagedUsageHistory.ID
FROM PackagedUsageHistory
INNER JOIN Users
on PackagedUsageHistory.userSid = Users.ID
INNER JOIN Capabilities
on PackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN PackageFamilyNames
on PackagedUsageHistory.PackageFamilyName = PackageFamilyNames.ID
INNER JOIN AppNames
On PackagedUsageHistory.AppName = AppNames.ID
"""
        headers_L = ('First_used_stop','Last_used_stop','AccessBlocked','Capability','AppName','PackageName','User SID','ID')
    else:
        Error_S = f"[ERROR][Query {query_name}] Non-supported version for '{db_version}'"
        logfunc(Error_S)

        sql_S = ""
        headers_L = []

    return sql_S, headers_L



#--------------------------------------------------------------------
# Returns the SQL for querying 'NonPackagedUsageHistory'
# @param db_version [IN] 
# @return SQL_query, list with columns names
# (Empty string and empty list if db_version is unknown)
# not supported
# 2025-04-16
# Query "B"
#--------------------------------------------------------------------
def get_SQL_NonpackagedUsageHistory(db_version, query_name):
    """Returns the core SQL code for querying 'NonPackageUsageHistory'"""
    # NOTE: there are some overlap in the two SQL queries, and so we 
    # could have a common part to be merged with the specific
    # part of each query. Having two separate queries in the function
    # allows for an easy test (just copy-paste into the SQL engine)

    if db_version in (C_W23H2, C_W23H2_DIFF):
        sql_S = """SELECT CASE WHEN LastUsedTimeStart = 0 THEN 'N.A.'
            ELSE datetime( (LastUsedTimeStart / 10000000) - 11644473600, 'unixepoch', 'localtime') 
        END AS Last_used_start,
datetime( (LastUsedTimeStop / 10000000) - 11644473600, 'unixepoch', 'localtime') AS Last_used_stop, 
CASE WHEN NonPackagedUsageHistory.AccessBlocked = 0 THEN 'Access OK'
ELSE 'Blocked'
END AS Access,
Capabilities.StringValue as Capability_str, 
BinaryFullPaths.StringValue as Bin_full_path,
FileIDs.StringValue as FileID,
ProgramIDs.StringValue as ProgramID,
Users.StringValue as UserSID,
NonPackagedUsageHistory.ID
FROM NonPackagedUsageHistory
INNER JOIN Users
on NonPackagedUsageHistory.userSid = Users.ID
INNER JOIN Capabilities
on NonPackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN BinaryFullPaths
on NonPackagedUsageHistory.BinaryFullPath = BinaryFullPaths.ID
INNER JOIN ProgramIDs
on NonPackagedUsageHistory.ProgramID = ProgramIDs.ID
INNER JOIN FileIDs
on NonPackagedUsageHistory.FileID = FileIDs.ID
"""
        headers_L = ('Last_used_start','Last_used_stop', 'Access', 
                     'Capability','Binary_full_path','FileID','ProgramID','UserSID','ID')

    elif db_version in (C_W24H2, C_W24H2_DIFF):
        sql_S = """SELECT CASE WHEN LastUsedTimeStart = 0 THEN 'N.A.'
            ELSE datetime( (LastUsedTimeStart / 10000000) - 11644473600, 'unixepoch', 'localtime') 
        END AS Last_used_start,
datetime( (LastUsedTimeStop / 10000000) - 11644473600, 'unixepoch', 'localtime') AS Last_used_stop, 
CASE WHEN NonPackagedUsageHistory.AccessBlocked = 0 THEN 'Access OK'
ELSE 'Blocked'
END AS Access,
Capabilities.StringValue as Capability_str, 
AppNames.StringValue as AppName,
BinaryFullPaths.StringValue as Bin_full_path,
FileIDs.StringValue as FileID,
ProgramIDs.StringValue as ProgramID,
Users.StringValue as UserSID,
NonPackagedUsageHistory.ID
FROM NonPackagedUsageHistory
INNER JOIN Users
on NonPackagedUsageHistory.userSid = Users.ID
INNER JOIN Capabilities
on NonPackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN BinaryFullPaths
on NonPackagedUsageHistory.BinaryFullPath = BinaryFullPaths.ID
INNER JOIN ProgramIDs
on NonPackagedUsageHistory.ProgramID = ProgramIDs.ID
INNER JOIN FileIDs
on NonPackagedUsageHistory.FileID = FileIDs.ID
INNER JOIN AppNames
on NonPackagedUsageHistory.AppName = AppNames.ID
"""
        headers_L = ('Last_used_start','Last_used_stop','Access','Capability','AppName',
                     'Binary_full_path','FileID','ProgramID','UserSID','ID')
    else:
        Error_S = f"[ERROR][Query '{query_name}'] Non-supported version for '{db_version}'"
        logfunc(Error_S)

        sql_S = ""
        headers_L = []

    return sql_S, headers_L

#--------------------------------------------------------------------
# Returns the SQL for querying 'NonPackagedUsageHistory'
# @param db_version [IN] C_W23H2,C_W23H2_DIFF / C_W24H2,C_W24H2_DIFF
# @param query_name [IN] Name of the query (for info)
# @return SQL_query, list with columns names
# (Empty string and empty list if db_version is unknown)
# not supported
# 2025-04-16
# Query "X2"
#--------------------------------------------------------------------
def get_SQL_NonpackagedUsageHistory_first_last(db_version, query_name):
    """Returns the core SQL code for querying 'NonPackageUsageHistory' FIRST/LAST"""
    # NOTE: there are some overlap in the two SQL queries, and so we 
    # could have a common part to be merged with the specific
    # part of each query. Having two separate queries in the function
    # allows for an easy test (just copy-paste into the SQL engine)

    if db_version in (C_W23H2, C_W23H2_DIFF):
        sql_S = """SELECT MIN(datetime((LastUsedTimeStop/10000000)-11644473600,'unixepoch','localtime')) as Last_used_stop_min, 
MAX(datetime( (LastUsedTimeStop / 10000000) - 11644473600, 'unixepoch', 'localtime')) as Last_used_stop_max, 
BinaryFullPaths.StringValue as bin_full_paths, 
Capabilities.StringValue as Capability_str,
NonPackagedUsageHistory.BinaryFullPath, 
Capabilities.StringValue
FROM NonPackagedUsageHistory
INNER JOIN BinaryFullPaths
on NonPackagedUsageHistory.BinaryFullPath = BinaryFullPaths.ID
INNER JOIN Capabilities
on NonPackagedUsageHistory.Capability = Capabilities.ID"""
        headers_L = ('First_used_stop', 'Last_used_stop', 'Bin_full_paths', 'Capability')   

    elif db_version in (C_W24H2, C_W24H2_DIFF):
        sql_S = """SELECT MIN(datetime((LastUsedTimeStop/10000000)-11644473600,'unixepoch','localtime')) as Last_used_stop_min, 
MAX(datetime( (LastUsedTimeStop / 10000000) - 11644473600, 'unixepoch', 'localtime')) as Last_used_stop_max, 
BinaryFullPaths.StringValue as bin_full_paths, 
Capabilities.StringValue as Capability_str,
NonPackagedUsageHistory.BinaryFullPath,
Capabilities.StringValue,
AppNames.StringValue
FROM NonPackagedUsageHistory
INNER JOIN BinaryFullPaths
on NonPackagedUsageHistory.BinaryFullPath = BinaryFullPaths.ID
INNER JOIN Capabilities
on NonPackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN AppNames
on NonPackagedUsageHistory.AppName = AppNames.ID
"""
        headers_L = ('First_used_stop','Last_used_stop','Bin_full_paths','Capability','AppName')   
    else:
        Error_S = f"[ERROR][Query '{query_name}'] Non-supported version for '{db_version}'"
        logfunc(Error_S)

        sql_S = ""
        headers_L = []

    return sql_S, headers_L


#--------------------------------------------------------------------
# Returns the SQL for querying 'NonPackagedUsageHistory'
# @param db_version [IN] C_W23H2,C_W23H2_DIFF / C_W24H2,C_W24H2_DIFF
# @param query_name [IN] Name of the query (for info)
# @return SQL_query, list with columns names
# (Empty string and empty list if db_version is unknown)
# not supported
# 2025-04-16
# Query "C"
#--------------------------------------------------------------------
def get_SQL_NonpackagedIdentityRelationship(db_version, query_name):
    """Returns the core SQL code for querying 'NonPackagedIdentityRelationship'"""
    # NOTE: there are some overlap in the two SQL queries, and so we 
    # could have a common part to be merged with the specific
    # part of each query. Having two separate queries in the function
    # allows for an easy test (just copy-paste into the SQL engine)

    # There is no difference from W23H2 vs. W24H2
    if db_version in (C_W23H2, C_W23H2_DIFF, C_W24H2, C_W24H2_DIFF):
        sql_S = """SELECT datetime((LastObservedTime/10000000)-11644473600,'unixepoch','localtime') AS Last_observed_time,
BinaryFullPaths.StringValue as Bin_full_Path,
ProgramIDs.StringValue as Program_hash,
NonPackagedIdentityRelationship.ProgramID,
FileIDs.StringValue as File_hash,
NonPackagedIdentityRelationship.FileID
FROM NonPackagedIdentityRelationship
INNER JOIN BinaryFullPaths
ON NonPackagedIdentityRelationship.BinaryFullPath = BinaryFullPaths.ID 
INNER JOIN ProgramIDs
ON NonPackagedIdentityRelationship.ProgramID = ProgramIDs.ID
INNER JOIN FileIDs
ON NonPackagedIdentityRelationship.FileID = FileIDs.ID"""
        headers_L = ('Last_observed_time', 'Bin_full_path', 'Program_hash', 'File_ID_hash', 'File_ID')   

    else:
        Error_S = f"[ERROR][Query '{query_name}'] Non-supported version for '{db_version}'"
        logfunc(Error_S)

        sql_S = ""
        headers_L = []

    return sql_S, headers_L


#--------------------------------------------------------------------
# Returns the SQL for UNION query packagedApps and nonPackagedApps
# @param db_version [IN] C_W23H2,C_W23H2_DIFF / C_W24H2,C_W24H2_DIFF
# @param query_name [IN] Name of the query (for info)
# @return SQL_packagedApps, SQL_nonPackagedApps, list with columns names
# (Empty strings and empty list if db_version is unknown)
# not supported
# 2025-04-16
# Query "D"
#--------------------------------------------------------------------
def get_SQL_history_all_applications(db_version, query_name):
    """Returns the core SQL code union packaged/non-packaged history"""
    # NOTE: there are some overlap in the two SQL queries, and so we 
    # could have a common part to be merged with the specific
    # part of each query. Having two separate queries in the function
    # allows for an easy test (just copy-paste into the SQL engine)

    if db_version in (C_W23H2, C_W23H2_DIFF):
        sql_packagedApps_S = """
SELECT datetime(datetime((LastUsedTimeStop/10000000) - 11644473600,'unixepoch','localtime')) AS Last_used_stop, 
CASE WHEN PackagedUsageHistory.AccessBlocked = 0 THEN 'Access OK'
ELSE 'Blocked'
END AS AccessBlocked,
Capabilities.StringValue as Capability_str,
PackageFamilyNames.StringValue as application_identifier,
Users.StringValue as UserSID
FROM PackagedUsageHistory
INNER JOIN Users
on PackagedUsageHistory.userSid = Users.ID
INNER JOIN Capabilities
on PackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN PackageFamilyNames
on PackagedUsageHistory.PackageFamilyName = PackageFamilyNames.ID"""

        sql_nonPackagedApps_S = """
SELECT datetime((LastUsedTimeStop/10000000) - 11644473600,'unixepoch','localtime') AS Last_used_stop, 
CASE WHEN NonPackagedUsageHistory.AccessBlocked = 0 THEN 'Access OK'
ELSE 'Blocked'
END AS AccessBlocked,
Capabilities.StringValue as Capability_str, 
BinaryFullPaths.StringValue as application_identifier,
Users.StringValue as UserSID
FROM NonPackagedUsageHistory
INNER JOIN Users
on NonPackagedUsageHistory.userSid = Users.ID
INNER JOIN Capabilities
on NonPackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN BinaryFullPaths
on NonPackagedUsageHistory.BinaryFullPath = BinaryFullPaths.ID"""

        headers_L = ('LastUsedTimeStop','AccessBlocked','Capability','AppIdentifier','UserSID')

    elif db_version in (C_W24H2, C_W24H2_DIFF):
        sql_packagedApps_S = """
SELECT datetime(datetime((LastUsedTimeStop/10000000) - 11644473600,'unixepoch','localtime')) AS Last_used_stop, 
CASE WHEN PackagedUsageHistory.AccessBlocked = 0 THEN 'Access OK'
ELSE 'Blocked'
END AS AccessBlocked,
Capabilities.StringValue as Capability_str,
PackageFamilyNames.StringValue as application_identifier,
AppNames.StringValue as AppName,
Users.StringValue as UserSID
FROM PackagedUsageHistory
INNER JOIN Users
on PackagedUsageHistory.userSid = Users.ID
INNER JOIN Capabilities
on PackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN PackageFamilyNames
on PackagedUsageHistory.PackageFamilyName = PackageFamilyNames.ID
INNER JOIN AppNames
on PackagedUsageHistory.AppName = AppNames.ID"""

        sql_nonPackagedApps_S = """
SELECT datetime((LastUsedTimeStop/10000000) - 11644473600,'unixepoch','localtime') AS Last_used_stop, 
CASE WHEN NonPackagedUsageHistory.AccessBlocked = 0 THEN 'Access OK'
ELSE 'Blocked'
END AS AccessBlocked,
Capabilities.StringValue as Capability_str, 
BinaryFullPaths.StringValue as application_identifier,
AppNames.StringValue as AppName,
Users.StringValue as UserSID
FROM NonPackagedUsageHistory
INNER JOIN Users
on NonPackagedUsageHistory.userSid = Users.ID
INNER JOIN Capabilities
on NonPackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN BinaryFullPaths
on NonPackagedUsageHistory.BinaryFullPath = BinaryFullPaths.ID
INNER JOIN AppNames
on NonPackagedUsageHistory.AppName = AppNames.ID"""

        headers_L = ('LastUsedTimeStop','Access','Capability', 
                     'AppIdentifier','AppName','UserSID')

    else:
        Error_S = f"[ERROR][Query '{query_name}'] Non-supported version for '{db_version}'"
        logfunc(Error_S)

        sql_packagedApps_S = ""
        sql_nonPackagedApps_S = ""
        headers_L = []

    return sql_packagedApps_S, sql_nonPackagedApps_S, headers_L

#--------------------------------------------------------------------
# Returns the SQL for querying 'NonPackagedGlobalPromptHistory'
# @param db_version [IN] 
# @return SQL_query, list with columns names
# (Empty string and empty list if db_version is unknown)
# NOTE: table does not exist for 'NonPackagedGlobalPromptHistory'
# 2025-04-16
# Query "G"
#--------------------------------------------------------------------
def get_SQL_NonpackagedGlobalPromptHistory(db_version, query_name, debug_flag=False):
    """Returns the core SQL code for querying 'NonPackagedGlobalPromptHistory'"""

    if debug_flag:
        Debug_S = f"[DEBUG] get_SQL_NonpackagedGlobalPromptHistory:{db_version},{query_name}"
        logfunc(Debug_S)

    if db_version in (C_W23H2, C_W23H2_DIFF):
        # The 'NonPackagedGlobalPromptHistory' does not exist for W23H2
        sql_S = ""
        headers_L = []

    elif db_version in (C_W24H2, C_W24H2_DIFF):
        sql_S = """SELECT CASE WHEN ShownTime = 0 THEN 'N.A.'
    ELSE datetime( (ShownTime/10000000)-11644473600, 'unixepoch', 'localtime') 
END AS PromptShownTime,
Capabilities.StringValue as Capability_Str,
Users.StringValue as UserSID,
FileIDs.StringValue as FileID,
ProgramIDs.StringValue as ProgramID,
NonPackagedGlobalPromptHistory.ID
FROM NonPackagedGlobalPromptHistory
INNER JOIN Users
on NonPackagedGlobalPromptHistory.userSid = Users.ID
INNER JOIN Capabilities
on NonPackagedGlobalPromptHistory.Capability = Capabilities.ID
INNER JOIN ProgramIDs
on NonPackagedGlobalPromptHistory.ProgramID = ProgramIDs.ID
INNER JOIN FileIDs
on NonPackagedGlobalPromptHistory.FileID = FileIDs.ID"""

        headers_L = ('ShownTime','Capability', 'FileID','ProgramID','UserSID','ID') 
    else:
        Error_S = f"[ERROR][Query '{query_name}'] Non-supported version for '{db_version}'"
        logfunc(Error_S)

        sql_S = ""
        headers_L = []

    return sql_S, headers_L


#--------------------------------------------------------------------
# Returns the two parts of a SQL query (to be joined with UNION ALL)
# The query aims to report on the count of capabilities per 
# application.
# @param db_version [IN] C_W23H2/C_W23H2_DIFF or C_W24H2/C_W24H2_DIFF
# @param query_name [IN] name of the query
# @param packaged_date_filter    [IN] string to add date_filter to sql_packagedApps_S. 
#                                     (empty string if date filtering is off)
# @param nonpackaged_date_filter [IN] string to add date_filter to sql_nonPackagedApps_S
#                                     (empty string if date filtering is off)
# @return sql_packagedApps_S, sql_nonPackagedApps_S, headers_L
# 2025-05-17
# Query "H"
#--------------------------------------------------------------------
def get_SQL_capability_per_app(db_version, query_name, packaged_date_filter, nonpackaged_date_filter):
    """Returns the SQL code for both the packaged and non-packaged capabilities per app"""

    if db_version in (C_W23H2, C_W23H2_DIFF):
        sql_packagedApps_S = f"""SELECT Capabilities.StringValue as Capability, 
CASE WHEN PackagedUsageHistory.AccessBlocked = 0 THEN 'Access OK'
ELSE 'Blocked'
END AS Access,
COUNT(*) as NumOccurrences, 
PackageFamilyNames.StringValue as PackageName,
Users.StringValue as UserSID
FROM PackagedUsageHistory
INNER JOIN Users
on PackagedUsageHistory.userSid = Users.ID
INNER JOIN Capabilities
on PackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN PackageFamilyNames
on PackagedUsageHistory.PackageFamilyName = PackageFamilyNames.ID
{packaged_date_filter} GROUP BY PackageName"""

        sql_nonPackagedApps_S = f"""SELECT Capabilities.StringValue as Capability, 
COUNT(*) as NumOccurrences,
CASE WHEN NonPackagedUsageHistory.AccessBlocked = 0 THEN 'Access OK'
ELSE 'Blocked'
END AS AccessBlocked,
BinaryFullPaths.StringValue as application_identifier,
Users.StringValue as UserSID
FROM NonPackagedUsageHistory
INNER JOIN Users
on NonPackagedUsageHistory.userSid = Users.ID
INNER JOIN Capabilities
on NonPackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN BinaryFullPaths
on NonPackagedUsageHistory.BinaryFullPath = BinaryFullPaths.ID
{nonpackaged_date_filter} GROUP BY application_identifier"""
        
        headers_L = ('Capability','Count','Access','Path/AppID','App','UserSID')

    elif db_version in (C_W24H2, C_W24H2_DIFF):
        sql_packagedApps_S = f"""SELECT Capabilities.StringValue as Capability,
COUNT(*) as NumOccurrences, 
CASE WHEN PackagedUsageHistory.AccessBlocked = 0 THEN 'Access OK'
ELSE 'Blocked'
END AS Access,
PackageFamilyNames.StringValue as PackageName,
"N.A.",
Users.StringValue as UserSID
FROM PackagedUsageHistory
INNER JOIN Users
on PackagedUsageHistory.userSid = Users.ID
INNER JOIN Capabilities
on PackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN PackageFamilyNames
on PackagedUsageHistory.PackageFamilyName = PackageFamilyNames.ID
{packaged_date_filter} GROUP BY AppName"""

        sql_nonPackagedApps_S = f"""SELECT Capabilities.StringValue as Capability, 
COUNT(*) as NumOccurrences, 
CASE WHEN NonPackagedUsageHistory.AccessBlocked = 0 THEN 'Access OK'
ELSE 'Blocked'
END AS AccessBlocked,
BinaryFullPaths.StringValue as application_identifier,
AppNames.StringValue as AppName,
Users.StringValue as UserSID
FROM NonPackagedUsageHistory
INNER JOIN Users
on NonPackagedUsageHistory.userSid = Users.ID
INNER JOIN Capabilities
on NonPackagedUsageHistory.Capability = Capabilities.ID
INNER JOIN BinaryFullPaths
on NonPackagedUsageHistory.BinaryFullPath = BinaryFullPaths.ID
INNER JOIN AppNames
on NonPackagedUsageHistory.AppName = AppNames.ID
{nonpackaged_date_filter} GROUP BY AppName"""
        headers_L = ('Capability','Count','Access','Path/AppID','App','UserSID')

    else:
        Error_S = f"[ERROR][Query '{query_name}'] Non-supported version for '{db_version}'"
        logfunc(Error_S)

        sql_packagedApps_S = ""
        sql_nonPackagedApps_S = ""
        headers_L = []

    return sql_packagedApps_S, sql_nonPackagedApps_S, headers_L        

#--------------------------------------------------------------------
# Checks if a file exists and if its size is greater than 0.
# filepath [IN] The path to the file to check.
# Return: True if the file exists and its size is greater than 0,
#         False otherwise.
# 2025-03-25
#--------------------------------------------------------------------
def check_file_exists_and_not_empty(filepath, debug_flag=False):
    if os.path.exists(filepath):
        if os.path.getsize(filepath) > 0:
            return True
        else:
            if debug_flag:
                Debug_S = f"[DEBUG] File '{filepath}' exists but is empty."
                logfunc(Debug_S)

            return False
    else:
        Debug_S = f"[DEBUG] File '{filepath}' does not exist."
        logfunc(Debug_S)
        return False


#--------------------------------------------------------------------
# 2025-03-22
#--------------------------------------------------------------------
def create_report_and_tsv_from_list(report_folder, name_S,  headers_L, data_L, 
                                    tsvname, file_found, dates_filter_str=None, debug_flag=False):
    """Execute the SQL query 'sql_S' over the SQLite3 cursor.
    'report_folder' [IN]: folder where to write the report
    'name_S'        [IN]: name of the report to be created
    'headers_L      [IN]: list of headers
    'tsvname'       [IN]: name of the TSV (Tab Separated Value) file
    """
    usageentries = len(data_L)

    if debug_flag:
        Debug_S = f"[DEBUG][{name_S}] usageentries={usageentries}"
        logfunc(Debug_S)

    if usageentries > 0:
        report = ArtifactHtmlReport(name_S, dates_filter_S = dates_filter_str)
        report.start_artifact_report(report_folder, name_S)
        report.add_script()

        data_list = []

        data_headers = headers_L
        for rows in data_L:
            try:
                payload = rows[0]
                payload_text = BeautifulSoup(payload, 'html.parser').text

                data_row_L = list(rows[1:])
                data_row_L.insert(0,payload_text)
                data_list.append(data_row_L)
            except Exception as e:
                print(f"ERROR:['{name_S}']: {e}")
                raise

        report.write_artifact_data_table(data_headers, data_list, file_found)
        report.end_artifact_report()

        if tsvname and len(tsvname) > 0:
            # Create TSV file
            tsv(report_folder, data_headers, data_list, tsvname)
    else:
        logfunc(f"No result for '{name_S}'")


#--------------------------------------------------------------------
# 2025-03-22
#--------------------------------------------------------------------
def dict_to_sorted_list(input_dict):
    """
    Converts a dictionary to a sorted list of tuples, sorted by the dictionary values.
    :param input_dict: Dictionary to be converted.
    :return: Sorted list of tuples (key, value).
    """
    # Use sorted() with a lambda function to sort by the dictionary values
    sorted_list = sorted(input_dict.items(), key=lambda item: item[1])
    return sorted_list

#--------------------------------------------------------------------
# 2025-03-22
#--------------------------------------------------------------------
def execute_sql_query(db_cursor, query_S):
    """
    Executes a SQL query and returns the results in a list of tuples.

    :param database: Path to the SQLite database file.
    :param query: SQL query to execute.
    :return: List of tuples containing the query results.
    """
    results_L = []

    try:
        # Execute the query
        db_cursor.execute(query_S)

        # Fetch all results
        results_L = db_cursor.fetchall()

    except sqlite3.Error as e:
        Err_S = f"[ERROR] Cannot exec SQL query:{e}"
        log_and_print_error(Err_S)

    return results_L


#--------------------------------------------------------------------
# 2025-03-22
#--------------------------------------------------------------------
def list_of_tuples_to_dict(list_of_tuples):
    """
    Converts a list of tuples to a dictionary.
    :param list_of_tuples: List of tuples where each tuple contains a key (string) and a value (integer).
    :return: Dictionary with keys and values from the list of tuples.
    """
    result_dict = {}
    for item in list_of_tuples:
        key = item[0]
        value = item[1]
        result_dict[key] = value

    return result_dict


#--------------------------------------------------------------------
# 2025-03-22
#--------------------------------------------------------------------
def merge_dicts_with_sum(dict1, dict2):
    """
    Merges two dictionaries. If both dictionaries have the same key, the values are added together.
    :param dict1: First dictionary.
    :param dict2: Second dictionary.
    :return: Merged dictionary with summed values for repeated keys.
    """
    merged_dict = dict1.copy()  # Create a copy of the first dictionary

    for key, value in dict2.items():
        if key in merged_dict:
            merged_dict[key] += value  # Add the values if the key exists in both dictionaries
        else:
            merged_dict[key] = value  # Otherwise, add the key-value pair from the second dictionary

    return merged_dict


#--------------------------------------------------------------------
# Return current source filename (for debugging)
# 2025-05-01
#--------------------------------------------------------------------
def __file__():
    return os.path.basename(__file__)

#--------------------------------------------------------------------
# Opens an SQLite database connection with exception handling, without 
# using the 'with' statement.
# Args:
#  db_file_path (str): The path to the SQLite database file.
# Returns:
#  sqlite3.Connection or None: The OPEN connection object if successful,
#                              None otherwise. The caller is responsible
#                              for closing the returned connection.
# 2025-04-30
#--------------------------------------------------------------------
def open_sqlite_db(db_file_path, debug_flag=False):
    """
   """

    """
    if is_platform_windows():
        if db_file_path.startswith('\\\\?\\UNC\\'): # UNC long path
            db_file_path = "%5C%5C%3F%5C" + db_file_path[4:]
        elif db_file_path.startswith('\\\\?\\'):    # normal long path
            db_file_path = "%5C%5C%3F%5C" + db_file_path[4:]
        elif db_file_path.startswith('\\\\'):       # UNC path
            db_file_path = "%5C%5C%3F%5C\\UNC" + db_file_path[1:]
##        else:                               # normal path
##            db_file_path = "%5C%5C%3F%5C" + db_file_path

"""
    # DEBUG
    if debug_flag:
        logfunc(f"[DEBUG][{__file__()}] Path for DB:'{db_file_path}'")

    connection = None # Initialize connection variable
    try:
        if debug_flag:
            print(f"[DEBUG][{__file__()}]Successfully connected to database: {db_file_path}")

        connection = sqlite3.connect(db_file_path)

        try:
            if debug_flag:
                cursor = connection.cursor()
                cursor.execute("SELECT sqlite_version();")
                version = cursor.fetchone()
                print(f"[INFO] SQLite engine version: {version[0]}")
                cursor.close()
        except sqlite3.Error as e_inner:
            # Handle errors during initial operations if necessary
            print(f"[WARN] Error during initial check after connect: {e_inner}")

        return connection

    except sqlite3.OperationalError as e:
        # Handles errors like permissions issues, disk full, file not found (if flags used), etc.
        print(f"[ERROR] Operational error connecting to database '{db_file_path}': {e}")
        # If an error occurred DURING connect, 'connection' might be None or partially formed.
        # Closing it might not be necessary or could even raise another error,
        # but it's safer to attempt cleanup if it got assigned somehow before failing.
        if connection:
            try:
                print("[WARN] Attempting to close potentially partially opened connection (DB:{db_file_path})")
                connection.close()
            except sqlite3.Error as close_err:
                 print(f"[WARN] Error closing connection after initial failure: {close_err}")
        return None # Indicate failure

    except sqlite3.DatabaseError as e:
        # Handles errors related to the database file itself (e.g., corruption)
        print(f"[ERROR] Database error for '{db_file_path}': {e}")
        if connection: # Attempt cleanup if connection object exists
             try: connection.close()
             except: pass # Ignore errors during cleanup after main error
        return None # Indicate failure

    except sqlite3.Error as e:
        # Catches any other SQLite-specific errors during connect
        print(f"[ERROR] An SQLite error occurred with '{db_file_path}': {e}")
        if connection:
             try: connection.close()
             except: pass
        return None # Indicate failure

    except Exception as e:
        # Catches any non-SQLite exceptions that might occur
        print(f"[ERROR][{__file__()}] An unexpected non-SQLite error occurred: {e}")
        if connection: # Attempt cleanup
             try: connection.close()
             except: pass
        return None # Indicate failure

#--------------------------------------------------------------------
# 2025-04-30
#--------------------------------------------------------------------
class AppConfig:
    """
    Encapsulates the processing of the JSON configuration file for the
    Capability Access Manager module.
    """
    #--------------------------------------------------------------------
    # Initializes the configuration object by loading and parsing the JSON file.
    # @param config_fname (str, optional): Path to the configuration file.
    #                                    Defaults to CONFIG_FNAME.
    # @param logger (callable, optional): Logging function to use.
    #                                     Defaults to a simple print function.
    # @return
    # 2025-04-18
    #--------------------------------------------------------------------
    def __init__(self, config_fname, logger=logfunc):
        """constructor"""
        self.log = logger
        self.config_file_path = config_fname
        self.config_loaded = False

        # --- Initialize attributes with default values ---
        self.start_date                      = None
        self.end_date                        = None
        self.start_date_ftime64              = None
        self.end_date_ftime64                = None
        self.dates_filter_S                  = None
        self.order_by_date                   = False
        self.show_SQL_flag                   = False
        self.count_per_category_flag         = False
        self.merge_WAL_file_to_DB_flag       = False
        self.merge_WAL_file_to_DB_debug_flag = False
        self.csv_amcache_path                = None
        self.save_SQL_to_file                = None

        # --- Add any other config attributes here with defaults ---

        # --- Load and process the configuration ---
        self._load_and_process()

    def _get_value(self, config_obj, json_path, log_prefix="[CONFIG]"):
        """Internal helper to get value and log if found."""
        value = get_config_value(config_obj, json_path)
        if value is not None:
            self.log(f"{log_prefix} {json_path}: '{value}'")
        return value

    def _load_and_process(self):
        """Loads the JSON config and populates the attributes."""
        config_obj = load_json_config(self.config_file_path)

        if config_obj is None:
            self.log(f"[INFO] no config file '{self.config_file_path}' or invalid JSON. Using defaults.")
            self.config_loaded = False
            # Attributes already have default values from __init__
            return
        else:
            self.log(f"[INFO] config file OK '{self.config_file_path}'")
            self.config_loaded = True

        # --- Date Range ---
        self.start_date = self._get_value(config_obj, "date_range.start_date")
        start_date_S = "--"
        if self.start_date is not None and (len(self.start_date) > 0) and self.start_date.upper() != "NONE":
            self.start_date_ftime64 = date_to_filetime(self.start_date)
            if self.start_date_ftime64 is not None:
                start_date_S = f"{self.start_date}"

        self.end_date = self._get_value(config_obj, "date_range.end_date")
        end_date_S = "--"
        if self.end_date is not None and (len(self.end_date) > 0) and self.end_date.upper() != "NONE":
            self.end_date_ftime64 = date_to_filetime(self.end_date)
            if self.end_date_ftime64 is not None:
                end_date_S = f"{self.end_date}"

        if (self.start_date is None) and (self.end_date is None):
            self.dates_filter_S = None
        else:
            self.dates_filter_S = f"[{start_date_S},{end_date_S}]"

        # --- Order by Date ---
        order_by_date_val = self._get_value(config_obj, "date_range.order_by_date")
        if order_by_date_val is not None:
            # Assume any value means True, adjust if specific values needed (e.g., true/false strings)
            self.order_by_date = bool(order_by_date_val) # Simple bool conversion

        # --- Show SQL ---
        show_sql_val = self._get_value(config_obj, "debug.show_SQL")
        if show_sql_val is not None:
            self.show_SQL_flag = bool(show_sql_val) # Simple bool conversion
    
        
        # --- filename to save SQL query -- only if self.show_sql_flag is active
        if self.show_SQL_flag is not None:
            save_SQL_to_file_val = self._get_value(config_obj, "debug.save_SQL_to_file")
            if save_SQL_to_file_val is not None:
                self.save_SQL_to_file = save_SQL_to_file_val


        # --- Count Per Category ---
        count_cat_val = self._get_value(config_obj, "compute.count_per_category")
        if count_cat_val is not None:
            self.count_per_category_flag = bool(count_cat_val) # Simple bool conversion

        # --- Merge WAL ---
        merge_wal_val = self._get_value(config_obj, "database.merge_WAL_file_to_DB")
        if merge_wal_val is not None:
            self.merge_WAL_file_to_DB_flag = bool(merge_wal_val) # Simple bool conversion

        # --- Merge WAL Debug ---
        merge_wal_debug_val = self._get_value(config_obj, "database.merge_WAL_file_to_DB_debug")
        if merge_wal_debug_val is not None:
            self.merge_WAL_file_to_DB_debug_flag = bool(merge_wal_debug_val) # Simple bool conversion

        # --- AmCache CSV Path ---
        csv_path_val = self._get_value(config_obj, "amcache.csv_filename")
        if csv_path_val is not None:
            if os.path.isfile(csv_path_val):
                self.csv_amcache_path = csv_path_val
            else:
                self.log(f"[WARNING] CSV Amache file '{csv_path_val}' not found (specified in config).")
                self.csv_amcache_path = None # Reset to None if file doesn't exist
        # else: self.csv_amcache_path remains None (default)



    def __str__(self):
        """Returns a human-readable string representation of the configuration."""
        status = "Loaded" if self.config_loaded else "Not Loaded (Using Defaults)"
        # Use 'N/A' or similar for None values for clarity
        start_dt = self.start_date if self.start_date is not None else "N/A"
        end_dt = self.end_date if self.end_date is not None else "N/A"
        dt_filter = self.dates_filter_S if self.dates_filter_S is not None else "N/A"
        amcache = self.csv_amcache_path if self.csv_amcache_path is not None else "N/A"
        save_SQL_filename = self.save_SQL_to_file if self.save_SQL_to_file is not None else "N/A"

        return (
            f"--- AppConfig Summary ---\n"
            f"Config File: '{self.config_file_path}' ({status})\n"
            f"Date Range:\n"
            f"  Start Date: {start_dt}\n"
            f"  End Date:   {end_dt}\n"
            f"  Filter Str: {dt_filter}\n"
            f"  Order by Date: {self.order_by_date}\n"
            f"Flags:\n"
            f"  Show SQL:                {self.show_SQL_flag}\n"
            f"  Count per Category:      {self.count_per_category_flag}\n"
            f"  Merge WAL File:          {self.merge_WAL_file_to_DB_flag}\n"
            f"  Merge WAL Debug:         {self.merge_WAL_file_to_DB_debug_flag}\n"
            f"External Files:\n"
            f"  AmCache CSV Path: '{amcache}'\n"
            f"  Filename to save SQL:    '{self.save_SQL_to_file}'\n"
            f"-------------------------"
        )


#--------------------------------------------------------------------
# 2025-04-19
#--------------------------------------------------------------------
def create_debug_dir(report_folder):
    """Simply creates the DEBUG directory as a subdir in the 'report_folder'"""
    C_DEBUG_DIR = "DEBUG"   # This can be moved to the config file
    debug_dir = C_DEBUG_DIR
    return create_subdir_in_report_folder(report_folder, debug_dir)

#--------------------------------------------------------------------
# Function called from the outside (and thus acting as a kind 
# of "main" for this module).
# 2025-03-02
#--------------------------------------------------------------------
def get_windowsCapability(files_found, report_folder, seeker, wrap_text):

    config_filename = "wleap-WindowsAccess.json"
    # Load configuration using AppConfig class
    config = AppConfig(config_fname = config_filename, logger=logfunc)
    
    # load local variables with values from the config file (via the AppConfig object)
    dates_filter_S                  = config.dates_filter_S
    order_by_date                   = config.order_by_date
    show_SQL_flag                   = config.show_SQL_flag
    save_SQL_to_filename            = config.save_SQL_to_file
    count_per_category_flag         = config.count_per_category_flag
    merge_WAL_file_to_DB_flag       = config.merge_WAL_file_to_DB_flag
    merge_WAL_file_to_DB_debug_flag = config.merge_WAL_file_to_DB_debug_flag
    csv_amcache_path                = config.csv_amcache_path
    start_date_ftime64              = config.start_date_ftime64
    end_date_ftime64                = config.end_date_ftime64


    # DEBUG
    print(f"{start_date_ftime64=}")
    print(f"{end_date_ftime64=}")

    #--------------------------------------------
    # End of config file processing
    #--------------------------------------------

    debug_flag = False

    if debug_flag: 
        print(f"{get_sep()}")
        print(f"[DEBUG] List of dir/files to analyze ({len(files_found)} files)")
        pprint.pp(files_found)
        print(f"{get_sep()}")

    # Was DB found?
    DB_found_flag = False

    db_filename_S = "CapabilityAccessManager.db"
    for file_found in files_found:
        file_found = str(file_found)
        if not os.path.basename(file_found) == db_filename_S:
            continue

        # We have the DB file - raise the flag
        DB_found_flag = True
        logfunc(f"{get_sep()}")
        logfunc(f"[INFO] CAM DB file found:")
        logfunc(f"'{file_found}' (path={len(file_found)} chars)")
        logfunc(f"{get_sep()}")

        # Try to infere the version of the CAM SQLite 3 database
        db_version = infere_cam_db_version(file_found)

        if db_version == C_UNKNOWN:
            logfunc(f"[INFO] Unrecognized database version '{file_found}' -- skipping")
            continue

        # Are we attempting to merge WAL with main DB? 
        if merge_WAL_file_to_DB_flag:
            output_debug_L = []     # List to collect debug/info messages
            ret_sync = sync_WAL_with_DB(file_found, output_debug_L)
            if ret_sync:
                logfunc(f"[INFO] '{os.path.basename(file_found)}' synchronized with WAL")

                debug_dir = create_debug_dir(report_folder)
                if debug_dir is not None:
                    # Subdir created with success - Write output_debug_L
                    # First, set the filename
                    C_WAL_DEBUG = "WAL_state_analysis.txt"
                    fname_path = os.path.join(debug_dir, C_WAL_DEBUG)
                    fname_path = get_next_unused_name(fname_path)
                    content_description = "DB state before and after WAL"
                    # Dump the content of the list in the filename
                    write_list_to_file(output_debug_L, content_description, fname_path )

        # Open DB in read-only mode
        db = open_sqlite_db_readonly(file_found)
        cursor = db.cursor()

        # DEBUG
        # Info_S = f"[INFO] DB '{file_found}' opened in read-only mode"
        # logfunc(Info_S)

        CAM_version_S = f"[INFO] CAM DB is '{CAM_version_str(db_version)}'"
        logfunc(CAM_version_S)

        # Record the performed queries
        Query_dones_L = []

        #========================================
        # "A" - Packaged applications
        #========================================
        Id_alpha = 'A'
        Id_S = f'{Id_alpha}'
        name_S = f'{Id_S}_CAM_PackagedApps'

        sql_S, headers_L = get_SQL_packagedUsageHistory(db_version, name_S)

        # Are date filters on? 
        date_field = "PackagedUsageHistory.LastUsedTimeStop"


        where_date_SQL_S = start_date_and_end_date_to_sql(start_date_ftime64, end_date_ftime64, date_field)
        # DEBUG
        print(f"Query A: {where_date_SQL_S=}")

        if where_date_SQL_S is not None:
            # DEBUG
            print(f"Adding '{where_date_SQL_S=}")
            sql_S = sql_S + where_date_SQL_S

        # Order by date?
        if order_by_date is True:
            order_by_date_S = f"ORDER BY {date_field}"
            sql_S = sql_S + "\n" + order_by_date_S

        # DEBUG
        if show_SQL_flag is True:
            show_SQL(name_S, sql_S, report_folder, save_SQL_to_filename)

        tsvname = f'{Id_S}_CAM_PackagedApps'
        create_report_and_tsv(report_folder, cursor, name_S, sql_S, headers_L, tsvname, file_found, dates_filter_S)

        # Query done
        Query_dones_L.append(Id_alpha)

        #========================================
        # "B" - Non-packaged applications
        #========================================
        Id_alpha = 'B'
        Id_S = f'{Id_alpha}'
        name_S = f'{Id_S}_CAM_NonPackagedApps'

        sql_S, headers_L = get_SQL_NonpackagedUsageHistory(db_version, name_S)
        # Are date filters on?
        date_field = "NonPackagedUsageHistory.LastUsedTimeStop"
        where_date_SQL_S = start_date_and_end_date_to_sql(start_date_ftime64,end_date_ftime64,date_field)
        if where_date_SQL_S is not None:
            sql_S = sql_S + "\n" + where_date_SQL_S

        # Order by date?
        if order_by_date is True:
            order_by_date_S = f"ORDER BY {date_field}"
            sql_S = sql_S + "\n" + order_by_date_S

        # DEBUG
        if show_SQL_flag is True:
            show_SQL(name_S, sql_S, report_folder, save_SQL_to_filename)

        tsvname = f'{Id_S}_CAM_NonPackagedApps'
        create_report_and_tsv(report_folder, cursor, name_S, sql_S, 
                        headers_L, tsvname, file_found, dates_filter_S)
        # Query done
        Query_dones_L.append(Id_alpha)

        #========================================
        # "C" - NonPackagedIdentityRelationship
        #========================================
        Id_alpha = 'C'
        Id_S = f'{Id_alpha}'
        name_S = f'{Id_S}_CAM_NonPackagedId'

        sql_S,headers_L = get_SQL_NonpackagedIdentityRelationship(db_version, name_S)

        # Are date filters on?
        date_field = "NonPackagedIdentityRelationship.LastObservedTime"
        where_date_SQL_S = start_date_and_end_date_to_sql(start_date_ftime64,end_date_ftime64,date_field)
        if where_date_SQL_S is not None:
            sql_S = sql_S + "\n" + where_date_SQL_S

        # Order by date?
        if order_by_date is True:
            order_by_date_S = f"ORDER BY {date_field}"
            sql_S = sql_S + "\n" + order_by_date_S
        else:
            order_by_date_S = "ORDER BY Program_hash"

        # DEBUG
        if show_SQL_flag is True:
            show_SQL(name_S, sql_S, report_folder, save_SQL_to_filename)

        tsvname = f'{Id_S}_CAM_NonPackagedIdRelation'
        create_report_and_tsv(report_folder, cursor, name_S, sql_S, 
                headers_L, tsvname, file_found, dates_filter_S)

        # Query done
        Query_dones_L.append(Id_alpha)

        #========================================
        # "D" - UNION ALL - sorted by date/time
        #========================================
        Id_alpha = 'D'
        Id_S = f'{Id_alpha}'
        name_S = f'{Id_S}_CAM_AllApps'

        sql_packagedApps_S, sql_nonPackagedApps_S, headers_L =\
                get_SQL_history_all_applications(db_version, name_S)

        # Are date filters on?
        date_field1 = "PackagedUsageHistory.LastUsedTimeStop"
        where_date_SQL1_S = start_date_and_end_date_to_sql(start_date_ftime64,end_date_ftime64,date_field1)
        if where_date_SQL1_S is not None:
            sql_packagedApps_S = sql_packagedApps_S + "\n" + where_date_SQL1_S

        date_field2 = "NonPackagedUsageHistory.LastUsedTimeStop"
        where_date_SQL2_S = start_date_and_end_date_to_sql(start_date_ftime64,end_date_ftime64,date_field2)
        if where_date_SQL2_S is not None:
            sql_nonPackagedApps_S = sql_nonPackagedApps_S + "\n" + where_date_SQL2_S

        # Union ALL
        sql_Union_All = "UNION ALL"
        sql_S = sql_packagedApps_S + "\n" + sql_Union_All + "\n" + sql_nonPackagedApps_S

        # Order by date?
        if order_by_date is True:
            order_by_date_S = f"ORDER BY Last_used_stop"
            sql_S = sql_S + "\n" + order_by_date_S

        # DEBUG
        if show_SQL_flag is True:
            show_SQL(name_S, sql_S, report_folder, save_SQL_to_filename)

        tsvname = f'{Id_S}_CAM_allApps'
        create_report_and_tsv(report_folder, cursor, name_S, sql_S, 
                        headers_L, tsvname, file_found, dates_filter_S)

        # Query done
        Query_dones_L.append(Id_alpha)

        #========================================
        # "E" - Category count - All Apps
        # We run two queries, convert the list
        # of tuples for each one to dictionary and
        # then merge the dictionaries.
        #========================================
        if count_per_category_flag:
            Id_alpha = 'E'
            Id_S = f'{Id_alpha}'
            name_S = f'{Id_S}_CAM_CountPerCapability'

            #--------------------
            # Packaged Apps
            #--------------------
            sql_packagedApps_S = """SELECT Capabilities.StringValue as Capability, COUNT(*) as Count 
FROM PackagedUsageHistory
INNER JOIN Capabilities
on PackagedUsageHistory.Capability = Capabilities.ID"""
            # Are date filters on?
            date_field1 = "PackagedUsageHistory.LastUsedTimeStop"
            where_date_SQL1_S = start_date_and_end_date_to_sql(start_date_ftime64,end_date_ftime64,date_field1)
            if where_date_SQL1_S is not None:
                sql_packagedApps_S = sql_packagedApps_S + "\n" + where_date_SQL1_S

            # GROUP BY
            group_by_PackagedCapability_S = "GROUP BY PackagedUsageHistory.Capability"
            sql_packagedApps_S += "\n" + group_by_PackagedCapability_S

            # Execute the query
            PackagedApps_L = execute_sql_query(cursor, sql_packagedApps_S) 

            # Convert the list of tuples to a dict
            PackagedApps_D = list_of_tuples_to_dict(PackagedApps_L)

            #--------------------
            # Non packaged apps
            #--------------------
            sql_nonPackagedApps_S = """
SELECT Capabilities.StringValue as Capability, COUNT(*) as Count 
FROM NonPackagedUsageHistory
INNER JOIN Capabilities
on NonPackagedUsageHistory.Capability = Capabilities.ID"""

            date_field2 = "NonPackagedUsageHistory.LastUsedTimeStop"
            where_date_SQL2_S = start_date_and_end_date_to_sql(start_date_ftime64,end_date_ftime64,date_field2)
            if where_date_SQL2_S is not None:
                sql_nonPackagedApps_S = sql_nonPackagedApps_S + "\n" + where_date_SQL2_S

            # GROUP BY
            group_by_nonPackagedApps_S = "GROUP BY NonPackagedUsageHistory.Capability"
            sql_nonPackagedApps_S = sql_nonPackagedApps_S + "\n" + group_by_nonPackagedApps_S

            # Execute the query
            NonPackagedApps_L = execute_sql_query(cursor, sql_nonPackagedApps_S) 

            # Convert the list of tuples to a dict
            NonPackagedApps_D = list_of_tuples_to_dict(NonPackagedApps_L)

            # Merge the two dicts - PackagedApps_D + NonPackagedApps_D
            allApps_D = merge_dicts_with_sum(PackagedApps_D, NonPackagedApps_D)

            # Extract a sorted list from the dict
            allApps_sorted_L = dict_to_sorted_list(allApps_D)

            tsvname = "" # We're skipping TSV as our data are not from a SQL query
            headers_L = ('Capability', 'Count')
            create_report_and_tsv_from_list(report_folder, name_S,  headers_L, 
                                            allApps_sorted_L, tsvname, file_found, dates_filter_S)
            # Query done
            Query_dones_L.append(Id_alpha)

        #=========================================
        # "F" - NonPackagedGlobalPromptHistory
        # (It does not exist for W23H2)
        #=========================================
        if db_version in (C_W24H2,C_W24H2_DIFF):
            Id_alpha = 'F'
            Id_S = f'{Id_alpha}'
            name_S = f'{Id_S}_CAM_NonPackagedPrompt'

            sql_S, headers_L = get_SQL_NonpackagedGlobalPromptHistory(db_version, name_S)

            # Are date filters on?
            date_field = "NonPackagedGlobalPromptHistory.ShownTime"
            where_date_SQL_S = start_date_and_end_date_to_sql(start_date_ftime64,end_date_ftime64,date_field)
            if where_date_SQL_S is not None:
                sql_S = sql_S + "\n" + where_date_SQL_S

            # Order by date?
            if order_by_date is True:
                order_by_date_S = f"ORDER BY ShownTime"
                sql_S = sql_S + "\n" + order_by_date_S

            # DEBUG:FIXME:2025-05-12
##            if show_SQL_flag is True:
##                # DEBUG:FIXME:DELETE:2025-05-12
##                # logfunc(f"SQL:{name_S}\n{sql_S}")
            if show_SQL_flag is True:
                show_SQL(name_S, sql_S, report_folder, save_SQL_to_filename)

            tsvname = f'{Id_S}_CAM_NonPackagedPromptHistory'
            create_report_and_tsv(report_folder, cursor, name_S, sql_S, 
                            headers_L, tsvname, file_found, dates_filter_S)

            # Query done
            Query_dones_L.append(Id_alpha)

        #========================================
        # "G" - amcache_data
        # Only if a amcache CSV file is provided
        #========================================
        if csv_amcache_path is not None:
            report_ID = 'G'
            csv_db_table_name = "amcache_data"
            lookup_and_report_fileID_in_CSV(file_found, csv_amcache_path, csv_db_table_name, report_ID, report_folder)

            # Query done
            Query_dones_L.append(report_ID)


        #========================================
        # "H" - Number of occurrences per 
        # capability per applications. 
        # 2025-05-16
        #========================================
        Id_alpha = 'H'
        Id_S = f'{Id_alpha}'
        name_S = f'{Id_S}_CAM_caps_per_app'

        # Set date filters 
        # (if needed, i.e., if date filtering is on)
        date_field1 = "PackagedUsageHistory.LastUsedTimeStop"
        where_date_SQL1_S = start_date_and_end_date_to_sql(start_date_ftime64,end_date_ftime64,date_field1)
        if where_date_SQL1_S is None:
            where_date_SQL1_S = ""

        date_field2 = "NonPackagedUsageHistory.LastUsedTimeStop"
        where_date_SQL2_S = start_date_and_end_date_to_sql(start_date_ftime64,end_date_ftime64,date_field2)
        if where_date_SQL2_S is None:
            where_date_SQL2_S = ""    

        # Get the two parts of the SQL query
        sql_packagedApps_S, sql_nonPackagedApps_S, headers_L =\
                get_SQL_capability_per_app(db_version, name_S, where_date_SQL1_S, where_date_SQL2_S)

        # Union ALL + ORDER BY
        sql_order_by = "ORDER BY NumOccurrences DESC"
        sql_Union_All = "UNION ALL"
        sql_S = sql_packagedApps_S + "\n" + sql_Union_All + "\n" + sql_nonPackagedApps_S + "\n" + sql_order_by

        if show_SQL_flag is True:
            show_SQL(name_S, sql_S, report_folder, save_SQL_to_filename)

        tsvname = f'{Id_S}_CAM_caps_per_App'
        create_report_and_tsv(report_folder, cursor, name_S, sql_S, 
                            headers_L, tsvname, file_found, dates_filter_S)

        # Query done
        Query_dones_L.append(Id_alpha)

        #========================================
        # "X1" - Packaged applications FIRST/LAST
        # Currently not active.
        # 2025-05-02
        #========================================
        do_query_packaged_first_last = False
        if do_query_packaged_first_last:
            Id_alpha = 'X1'
            Id_S = f'{Id_alpha}'
            name_S = f'{Id_S}_[PackagedApps] First+Last Access'

            sql_S, headers_L = get_SQL_packagedUsageHistory_FirstLast(db_version, name_S)

            # GROUP BY statement
            group_by_S = "GROUP BY PackageFamilyNames.StringValue"

            # Are date filters on?
            date_field = "PackagedUsageHistory.LastUsedTimeStop"
            where_date_SQL_S = start_date_and_end_date_to_sql(start_date_ftime64,end_date_ftime64,date_field)
            if where_date_SQL_S is not None:
                sql_S = sql_S + "\n" + where_date_SQL_S

            # Add group by statement (which is non-related to date filtering)
            sql_S = sql_S + "\n" + group_by_S

            # Order by date?
            if order_by_date is True:
                order_by_date_S = f"ORDER BY {date_field}"
                sql_S = sql_S + "\n" + order_by_date_S

            # DEBUG:FIXME:2025-05-12
##            if show_SQL_flag is True:
##                sep_S = get_sep()
##                show_SQL = f"SQL:{name_S}\n{sql_S}{sep_S}"
##                logfunc(show_SQL)
            if show_SQL_flag is True:
                show_SQL(name_S, sql_S, report_folder, save_SQL_to_filename)

            tsvname = f'{Id_S}_CAM_PackagedApps_First+Last'
            create_report_and_tsv(report_folder, cursor, name_S, sql_S, headers_L, tsvname, file_found, dates_filter_S)

            # Query done
            Query_dones_L.append(Id_alpha)
            

        #========================================
        # "X2" - Non-packaged applications FIRST/LAST
        # Currently not active.
        # 2025-05-02
        #========================================
        do_query_nonpackaged_first_last = False
        if do_query_nonpackaged_first_last:
            Id_alpha = 'X2'
            Id_S = f'{Id_alpha}'
            name_S = f'{Id_S}_[Non-PackagedApps] First+Last Access'

            sql_S,headers_L = get_SQL_NonpackagedUsageHistory_first_last(db_version, name_S)

            # GROUP BY statement
            group_by_S = "GROUP BY NonPackagedUsageHistory.BinaryFullPath"

            # Are date filters on?
            date_field = "NonPackagedUsageHistory.LastUsedTimeStop"
            where_date_SQL_S = start_date_and_end_date_to_sql(start_date_ftime64,end_date_ftime64,date_field)
            if where_date_SQL_S is not None:
                sql_S = sql_S + "\n" + where_date_SQL_S

            # Add group by statement (which is non-related to date filtering)
            sql_S = sql_S + "\n" + group_by_S

            # Order by date?
            if order_by_date is True:
                order_by_date_S = f"ORDER BY {date_field}"
                sql_S = sql_S + "\n" + order_by_date_S

            if show_SQL_flag is True:
                show_SQL(name_S, sql_S, report_folder, save_SQL_to_filename)

            tsvname = f'{Id_S}_CAM_NonpackagedApps_First+Last'
            create_report_and_tsv(report_folder, cursor, name_S, sql_S,
                                  headers_L, tsvname, file_found, dates_filter_S)
            # Query done
            Query_dones_L.append(Id_alpha)

        #====================
        # Done
        #====================
        if not DB_found_flag:
            info_S = f"No Windows Capability Access Manager data available ('{db_filename_S}' not found)."
            logfunc(info_S)

        db.close()

#====================================================================
# "Misc" functions.
#====================================================================

#--------------------------------------------------------------------
# Show SQL query 'sql_query_S' (whose name is 'name') to sdout, 
# and if save_SQL_filename is defined, content is also written 
# to 'save_SQL_filename'
# @param name_query_S [IN] Name of the query
# @param sql_query_S  [IN] SQL query
# @param save_SQL_filename [IN] filename where the SQL should be saved
# @return None
# 2025-05-12
#--------------------------------------------------------------------
def show_SQL(name_query_S, sql_query_S, report_folder, save_SQL_filename):
    "Show SQL query and save if save_SQL_filename is not None"
    Sep_S = get_sep()
    Show_SQL_S = f"{Sep_S}\nSQL:{name_query_S}\n{sql_query_S}\n{Sep_S}"
    logfunc(Show_SQL_S)

    if save_SQL_filename is not None:
        debug_dir = create_debug_dir(report_folder)
        if debug_dir is not None:
            fname_path = os.path.join(debug_dir, save_SQL_filename)
            with open(fname_path, 'a', encoding='utf-8') as file:
                file.write(Show_SQL_S + '\n')

#--------------------------------------------------------------------
# 2025-04-19
#--------------------------------------------------------------------
def create_subdir_in_report_folder(report_folder, sub_dirname):
    """
    Creates a subdirectory as a sibling to the specified report_folder.

    For example, if report_folder is '/path/to/reports/my_report' and
    sub_dirname is 'details', this function will create '/path/to/reports/details'
    if it doesn't already exist.

    Args:
        report_folder (str): The path to the reference folder. The new subdirectory
                             will be created in the parent directory of this folder.
        sub_dirname (str): The name of the subdirectory to create.

    Returns:
        str: The absolute path to the created (or existing) subdirectory.
             Returns None if input paths are invalid or errors occur during creation
             
    Raises:
        OSError: If there are permission issues or other OS-level errors
                 during directory creation.
    """
    # Ensure consistent path formatting by removing any trailing slashes (both kinds)
    # This prevents issues with os.path.split if the path ends with a separator.
    report_folder = report_folder.rstrip('/') 
    report_folder = report_folder.rstrip('\\') 

    # Split the report_folder path into its directory part (head) and the last component (tail).
    # For '/path/to/reports/my_report', report_folder_base becomes '/path/to/reports'
    # and tail becomes 'my_report' (tail is not used further in this function).
    report_folder_base, tail = os.path.split(report_folder) 

    # Construct the full path for the desired subdirectory.
    # It joins the parent directory of the original report_folder with the new subdirectory name.
    # e.g., os.path.join('/path/to/reports', 'details') -> '/path/to/reports/details'
    subdir_in_report_folder = os.path.join(report_folder_base, sub_dirname)

    # Check if the target subdirectory already exists.
    if os.path.isdir(subdir_in_report_folder):
        # If the directory already exists, do nothing.
        pass 
    else:
        # If the directory does not exist, create it.
        # os.makedirs() creates the directory and any necessary parent directories.
        # It's generally safer than os.mkdir() which only creates the final directory.
        try:
            os.makedirs(subdir_in_report_folder)
            # Optional: Add logging here if needed, e.g., logfunc(f"Created directory: {subdir_in_report_folder}")
        except OSError as e:
            logfunc(f"[ERROR] Cannot create dir '{subdir_in_report_folder}': {e}")
            return None

    # Return the full path to the subdirectory (either pre-existing or newly created).
    return subdir_in_report_folder



#--------------------------------------------------------------------
# @return True if removed, False otherwise (it might not exists)
# 2025-04-17
#--------------------------------------------------------------------
def delete_file_if_exists(filename,debug_flag=False):
    """Attempt to delete a file if it exists, otherwise fails silently"""

    # DEBUG
    if debug_flag:
        logfunc(f"[DEBUG] Attempt to delete '{filename}'")

    try:
        if os.path.exists(filename):
            os.remove(filename)
            return True
    except FileNotFoundError:
        # Ok, file not found: not a problem 
        # (this should not happen as we testing the existence of 'filename')
        pass
    except PermissionError:
        log_and_print_error(f"[ERROR] Permission denied: Cannot delete file '{filename}'.")
    except Exception as e:
        log_and_print_error(f"[ERROR] Exception while try to delete file '{filename}': {e}")

    return False

#--------------------------------------------------------------------
# 2025-03-19
#--------------------------------------------------------------------
def print_banner(text, char="*"):
    """Prints a simple banner around the given text.
    Args:
        text: The string to be displayed in the banner.
        char: The character used to create the banner border (default is "*").
    """
    n = len(text)
    border = char * (n + 4)
    print(border)
    print(f"{char} {text} {char}")
    print(border)

#--------------------------------------------------------------------
# 2025-03-26
#--------------------------------------------------------------------
def get_sep(sep_char="-"):
    "return a one line separator"
    return sep_char * 80

#====================================================================
# Code to deal with JSON configuration file
#====================================================================

#--------------------------------------------------------------------
# Load a JSON configuration file
# @config_file [IN] The path to the JSON configuration file.
# @return A dictionary containing the configuration data.
#         Returns None if the file cannot be loaded or parsed.
# 2025-03-16
#--------------------------------------------------------------------
def load_json_config(config_file):
    "Load a JSON configuration file"
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        Err_S = f"[INFO] Configuration file not found: {config_file} ({e})"
        log_and_print_error(Err_S)
        return None

    except json.JSONDecodeError as e:
        Err_S = f"[ERROR] Error parsing JSON file: {config_file}\n{e}"
        log_and_print_error(Err_S)
        return None

    except Exception as e:
        Err_S = f"[ERROR] Unexpected error: '{e}'"
        log_and_print_error(Err_S)
        return None


#--------------------------------------------------------------------
# 2025-03-19
#--------------------------------------------------------------------
def log_and_print_error(error_S):
    """log and print (banner) the error msg and the exception msg"""
    logfunc(error_S)
    print_banner(error_S)

#--------------------------------------------------------------------
# 2025-03-16
#--------------------------------------------------------------------
def get_config_value(config, key_path, default=None):
    """Retrieves a value from the configuration dictionary using a key path."""
    keys = key_path.split('.')
    current = config

    try:
        for key in keys:
            current = current[key]
        return current
    except (KeyError, TypeError):
        return default


#--------------------------------------------------------------------
#    Writes a list to a file, with each element on a new line.
#    Adds two header lines:
#    1. A description of the content.
#    2. The OS and version information.
#    
# @param data_list [IN] list of items to write to the file. Each item will be
#                   converted to a string and written on a separate line.
# @param content_description [IN] A string describing the contents of the list,
#                             used for the first header line.
# @param filename [IN] The path to the file to be created or overwritten.
# @return None
# 2025-04-19
#--------------------------------------------------------------------
def write_list_to_file(data_list: List[Any],content_description: str, filename: str) -> None:
    """Write 'data_list' to filename, adding the description 'content_description'"""
    try:
        # Get OS information using the platform module
        os_info = platform.platform()

        # Use a context manager to ensure the file is properly closed
        with open(filename, 'w', encoding='utf-8') as f:
            # Write the header lines, starting with '#' and ending with newline
            f.write(f"# {content_description}\n")
            f.write(f"# OS: {os_info}\n")

            # Iterate through the list and write each item on a new line
            for item in data_list:
                # Convert item to string just in case it's not already
                f.write(f"{str(item)}\n")

        # Optional: Log success if needed
        # print(f"Successfully wrote list to '{filename}'")
    except IOError as e:
        # Handle potential file writing errors (e.g., permissions)
        print(f"Error writing to file '{filename}': {e}")
        # Re-raise the exception if the caller needs to handle it
        raise
    except TypeError as e:
        # Handle potential errors if data_list isn't iterable or items aren't strings
        print(f"Error processing data for file '{filename}': {e}")
        raise
    except Exception as e:
        # Catch any other unexpected errors during file operations
        print(f"An unexpected error occurred while writing to '{filename}': {e}")
        raise


