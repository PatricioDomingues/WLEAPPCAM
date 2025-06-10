# SQLite Database Schema Inspector and Comparator

This Python script, `py_get_DB_schema.py`, is a command-line tool designed to inspect the schema of a SQLite database. Its primary function is to read a database file, extract its structure (tables and columns), and compare it against known, hardcoded schemas.

The script was specifically built to analyze the Windows **Capability Access Manager** database (`CapabilityAccessManager.db`), providing built-in schema definitions for Windows 11 versions **23H2** and **24H2**. This allows a user to quickly verify if a given `CapabilityAccessManager.db` file matches the expected structure for a particular Windows version.

## Key Features

- **Safe, Read-Only Access**: Connects to the SQLite database in read-only mode (`mode=ro`) to ensure the target file is never modified.
- **Schema Inspection**: Can list all tables and their corresponding columns.
- **Column Count Summary**: Can list all tables and provide a count of their columns.
- **Schema Comparison**: Compares the schema of a given database against built-in baselines for Windows 11 (23H2 and 24H2).
- **Difference Reporting**: Clearly reports any differences found during the comparison, including new tables, removed tables, or tables with a changed number of columns.
- **Simple Command-Line Interface**: Easy to use with standard command-line arguments.
- **Standard Libraries Only**: Requires only Python 3 and its standard libraries (`sqlite3`, `argparse`, etc.), so no external packages need to be installed.

## Requirements

- Python 3

## Usage

The script is run from the command line. You must provide the path to the SQLite database and specify which Windows version's schema to use for comparison. You also need to choose whether to list full column names or just the column count.

### Command-Line Arguments

```
usage: py_get_DB_schema.py [-h] --path PATH [--count] [--list] --windows WINDOWS_VERSION

Print the structure of the CapabilitAcessManager.db passed through the command line and compares it with the CapabilitAcessManager.db of W22H3 [9 tables].

options:
  -h, --help            show this help message and exit
  --path PATH, -p PATH  Path to SQLite3 database
  --count, -c           List tables and their number of columns
  --list, -l            List tables and their columns
  --windows WINDOWS_VERSION, -w WINDOWS_VERSION
                        Specify the Windows version (e.g., W23H2 or W24H2).
                        Case-insensitive options are also accepted.
```

### Examples

**1. List tables and column counts and compare against the W23H2 schema:**

```bash
python py_get_DB_schema.py --path C:\path\to\your\CapabilityAccessManager.db --count --windows W23H2
```
or using short-form flags:
```bash
python py_get_DB_schema.py -p cam.db -c -w W23H2
```

**2. List tables and full column names and compare against the W24H2 schema:**

```bash
python py_get_DB_schema.py --path /path/to/another/cam.db --list --windows W24H2
```
or using short-form flags:
```bash
python py_get_DB_schema.py -p cam.db -l -w W24H2
```

## How It Works

1.  **Connect to DB**: The script constructs a URI with `?mode=ro` to open the specified SQLite file in read-only mode. This prevents any accidental writes or modifications to the database, making it safe to use on live system files.
2.  **Extract Schema**: It queries the `sqlite_master` table to get a list of all user-defined tables.
3.  **Get Column Info**: For each table, it uses the `PRAGMA table_info("table_name");` command to retrieve details about each column.
4.  **Build Dictionary**: It constructs a Python dictionary representing the database schema.
    -   If using `--list`, the dictionary values are lists of alphabetically sorted column names.
    -   If using `--count`, the dictionary values are the integer count of columns for each table.
5.  **Compare Schemas**: The generated dictionary is compared against the corresponding hardcoded dictionary for the specified Windows version (`W23H2` or `W24H2`). The comparison checks for:
    -   Tables present in the database but not in the baseline (new keys).
    -   Tables present in the baseline but not in the database (removed keys).
    -   Tables present in both but with different values (i.e., different column lists or counts).
6.  **Print Results**: The script prints the extracted schema and the results of the comparison to the console.

## Understanding the Output

The script produces two main blocks of output separated by a line of equals signs (`=`).

### 1. Extracted Database Schema

The first block shows the schema extracted from the database file you provided. It includes file path, file size, the schema dictionary, the total number of tables, and a sorted list of table names.

****Example Output (with `--count`)****
================================================================================
```
[INFO] tables/# of columns of SQLite 3 database
Database path: 'CapabilityAccessManager.db' (36864 to 36864 bytes)
{'BinaryFullPaths': 2,
 'Capabilities': 2,
 'FileIDs': 2,
 'NonPackagedIdentityRelationship': 5,
 'NonPackagedUsageHistory': 9,
 'PackageFamilyNames': 2,
 'PackagedUsageHistory': 7,
 'ProgramIDs': 2,
 'Users': 2}
9 tables
['BinaryFullPaths', 'Capabilities', 'FileIDs', 'NonPackagedIdentityRelationship', 'NonPackagedUsageHistory', 'PackageFamilyNames', 'PackagedUsageHistory', 'ProgramIDs', 'Users']
================================================================================
```

### 2. Comparison Result

The second block shows the result of the comparison.

- If the schema **matches** the built-in baseline, the output will be `['OK']`.

  ```
  ['OK']
  ================================================================================

- If there are **differences**, the output will be a list of strings describing what changed.
**Example Output (with differences)**
```  
  ['New keys: newtable',
   'Removed Keys: oldtable',
   'Keys with changed values: NonPackagedUsageHistory']
  ================================================================================
  ```

---

