# Excel Ingestion Webapp

Created by Tyler Netherly (netherlyt@gmail.com)

## Setup Guide

This assumes you have a Snowflake account setup with ACCOUNTADMIN permissions.

1. Run ```1_Setup.sql``` in the ```Snowflake_Scripts``` folder. This will set up a role, warehouse, database, schema, and empty tables for the app.

2. Run ```2_IngestProcedure.sql```. This will create two stored procedures to be used with the two example files.

3. Setup a Streamlit app in the SnowSight. Use the resources we set up in Step 1 for this app. Copy and paste the code from ```Excel_Upload_App.py``` into the code editor.

4. Run ```3_NetworkSetup.sql```. You will need to obtain a dbt Cloud API key as well as the tenant ID and client app secret for the Power BI API. 

5. Fill out the missing values in the Streamlit app and you should be good to go! Feel free to remove the Power BI parts of the app if that is not applicable. It should also be easy to swap those parts out for a Tableau API call.

Feel free to reach out with questions!!