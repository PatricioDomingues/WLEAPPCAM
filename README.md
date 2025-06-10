# Module "windowsCapability" for WLEAPP

## How to install
1. Unzip the code
2. Install uv if not already installed: `pip install uv`
3. Create the virtual environment: `uv venv CAM` (replace "CAM" with any name you prefer)
4. Activate the virtual environment: `CAM\Scripts\activate`
5. Install requirements: `uv pip install -r requirements.txt`
6. WLEAPP should now be ready to run

## How to run the module

**Note:** Location of the "CapabilityAccessManager.db" SQLite3 database in Windows 11:
`%PROGRAMADATA%\Microsoft\Windows\CapabilityAccessManager\`
The database name is: `"CapabilityAccessManager.db"`


### Running with filesystem ("fs")
```
python wleapp.py -t fs -i DirCapabilityDB -o OUT -m windowsCapability
```

Where:
- `-t fs`: Use filesystem
- `-i InputDIR`: Input directory which must contain a directory called 'CapabilityAccessManager' that holds the "capabilityAccessManager.db"
- `-o OutputDIR`: Output directory where HTML reports and TSV files will be created
- `-m windowsCapability`: Name of WLEAPP's CAM module (mandatory)

### Running with a ZIP archive
```
python wleapp.py -t zip -i ArchiveFS.zip -o DIR -m windowsCapability
```

Where:
- `-t zip`: Input is a ZIP file
- `-i ArchiveFS.zip`: ZIP file holding a directory hierarchy that contains a directory called CapabilityAccessManager with a "capabilityAccessManager.db" database

## AMCACHE functionality (optional)

To access the AMcache comparison regarding FileID and ProgramID:

1. Run Eric Zimmermann's AmcacheParser.exe:
   ```
   AmcacheParser.exe -f "C:\Windows\appcompat\Programs\Amcache.hve" --csv CSV_Destination_DIR
   ```

2. Specify the CSV file '*unassociatedFileEntries.csv' in the 'wleap-WindowsAccess.json' file:
   ```json
   "amcache": {
     "csv_filename_comment": "CSV file created by E.Zimmerman's AmCache tool with the unassociated entries of the AmCache",
     "csv_filename": "Amcache_UnassociatedFileEntries.csv"
   }
   ```

**Note:** Location of Amcache on Windows 11: `C:\Windows\AppCompat\Programs`

## Directory Examples-DB

The directory `Examples-DB` has two ZIP archives -- `CAM_database_W11_23h2.zip` and `CAM_database_W11_24h2.zip`. Each one holds a CapabilityAccessManager.db database with activity data. One is from W11-23H2, the other one is from W11-24H2.

## Future
We plan to have the Capability Access Manager merged with the official WLEAPP (https://github.com/abrignoni/WLEAPP) in the future. 
(we just need to do some more work on CAM).
