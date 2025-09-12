# Data Automation Script

#### Migrate data from excel or Google sheets to AWS RDS

## Build the 'pipelines excel file' - lists all Source files by type (gsheet, excel)

#### Creates excel files for each table and it's data.

#### Creates CSV file for each table with no data - fill in 2nd row cells under each field for expected data type - looking at leveraging AI for this too to automate this step

#### Excel files will be used for the migration to RDS combined with CSV files that define the data types.

#### Excel sheet creates with each row of create table statements ready for destination migration

## Database server credentials

#### Store all credentials within environment variables.

### For Mac/Linux

#### e.g. EXPORT VARNAME="key"

### For Windows/Powershell

#### $env:VARNAME="key"
