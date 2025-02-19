CREATE OR REPLACE PROCEDURE APP_DB.APP_SCHEMA.IMPORT_COMPANY_A_PRICES("FILE_PATH" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas','openpyxl')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.files import SnowflakeFile
from openpyxl import load_workbook
import pandas as pd
from datetime import datetime
 
def main(session, file_path):
 with SnowflakeFile.open(file_path, ''rb'') as f:
    # read in header rows
    header = pd.read_excel(f, header=None, nrows=11)  

    # get the list date cell
    date_raw = header.iloc[2,6]

    # remove the ''Date: '' text
    date_raw_value = date_raw.split('': '')[1]

    # strip out the date value
    date_value = datetime.strptime(date_raw_value, ''%m/%d/%Y'').date()
    
    # read in the data
    data = pd.read_excel(f, header=11)

    # clean the data up

    # give the first column a name
    data.columns.values[0] = ''Special''

    # remove rows with an NA ID
    data = data.dropna(subset=[''ID''])

    # remove empty column at the end
    data = data.dropna(axis=1, how=''all'')

    # add date as LIST_DATE column
    data[''LIST_DATE''] = date_value

    # check Snowflake table to make sure list date is not already in the table
    current_dates = session.sql(''SELECT DISTINCT "LIST_DATE" FROM COMPANY_A_PRICE_LISTS'').to_pandas()

    if date_value in current_dates["LIST_DATE"].values:
        raise ValueError(f"The list for date {date_value} has already been uploaded. Please contact the Data Team for further assistance.")

    # write the data to Snowflake
    df2 = session.create_dataframe(data)

    df2.write.mode("append").save_as_table("COMPANY_A_PRICE_LISTS")

 return True
';

CREATE OR REPLACE PROCEDURE APP_DB.APP_SCHEMA.IMPORT_COMPANY_B_PRICES("FILE_PATH" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas','openpyxl')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.files import SnowflakeFile
from openpyxl import load_workbook
import pandas as pd
from datetime import datetime
 
def main(session, file_path):
 with SnowflakeFile.open(file_path, ''rb'') as f:
    # read in header rows
    header = pd.read_excel(f, header=None, nrows=11)  

    # get the list date cell
    date_raw = header.iloc[2,6]

    # remove the ''Date: '' text
    date_raw_value = date_raw.split('': '')[1]

    # strip out the date value
    date_value = datetime.strptime(date_raw_value, ''%m/%d/%Y'').date()
    
    # read in the data
    data = pd.read_excel(f, header=11)

    # clean the data up

    # give the first column a name
    data.columns.values[0] = ''Special''

    # remove rows with an NA ID
    data = data.dropna(subset=[''ID''])

    # remove empty column at the end
    data = data.dropna(axis=1, how=''all'')

    # add date as LIST_DATE column
    data[''LIST_DATE''] = date_value

    # check Snowflake table to make sure list date is not already in the table
    current_dates = session.sql(''SELECT DISTINCT "LIST_DATE" FROM COMPANY_B_PRICE_LISTS'').to_pandas()

    if date_value in current_dates["LIST_DATE"].values:
        raise ValueError(f"The list for date {date_value} has already been uploaded. Please contact the Data Team for further assistance.")

    # write the data to Snowflake
    df2 = session.create_dataframe(data)

    df2.write.mode("append").save_as_table("COMPANY_B_PRICE_LISTS")

 return True
';


-- Once created, test to make sure the procedure works
-- I've uploaded a test file to the Snowflake Stage

CALL APP_DB.APP_SCHEMA.IMPORT_PRICES(build_scoped_file_url(@APP_STAGE, 'example_excel_file1.xlsx'));

-- It works! Hallelujah!