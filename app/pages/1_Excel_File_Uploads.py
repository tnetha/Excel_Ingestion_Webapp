import os
from tempfile import NamedTemporaryFile
import streamlit as st
import snowflake.connector
from snowflake.snowpark import Session
import requests
import time

st.set_page_config(page_title="CountryMark Contracts File Import", layout="wide")

def get_connection_params():
    if os.path.isfile("/snowflake/session/token"):
        return {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'host': os.getenv('SNOWFLAKE_HOST'),
            'authenticator': "oauth",
            'token': open('/snowflake/session/token', 'r').read(),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'client_session_keep_alive': True
        }
    else:
        return {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'authenticator': 'username_password_mfa',
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'client_session_keep_alive': True
        }

def create_session() -> Session:
    connection_params = get_connection_params()
    return Session.builder.configs(connection_params).create()

# DBT Cloud API and Power BI API details
DBT_API_URL = 'https://cloud.getdbt.com/api/v2/accounts/143450/jobs/805062/run/'
DBT_JOB_STATUS_URL = 'https://cloud.getdbt.com/api/v2/accounts/143450/runs/{run_id}/'

# Trigger DBT Cloud job
def call_dbt_api():
    api_key = os.getenv('DBT_CLOUD_API_KEY')
    headers = {
        'Authorization': f'Token {api_key}'
    }
    body = {
        "cause": "Triggered via API"
    }
    response = requests.post(DBT_API_URL, json=body, headers=headers)

    # Print the full URL redirect path
    if response.history:
        print("Redirect history:")
        for resp in response.history:
            print(resp.url)
        print("Final destination URL:", response.url)
    else:
        print("No redirects, final URL:", response.url)
        
    if response.status_code == 200:
        return response.json()  # Get the run ID from the response
    else:
        st.error("Failed to trigger DBT job.")
        return None
    
# Check DBT Cloud job status
def check_dbt_job_status(run_id):
    api_key = os.getenv('DBT_CLOUD_API_KEY')
    headers = {
        'Authorization': f'Token {api_key}'
    }
    job_status_url = DBT_JOB_STATUS_URL.format(account_id=os.getenv('DBT_ACCOUNT_ID'), run_id=run_id)
    response = requests.get(job_status_url, headers=headers)
    if response.status_code == 200:
        return response.json()  # Returns job status
    else:
        st.error("Failed to check DBT job status.")
        return None
    
# Make connection to Snowflake and cache it
@st.cache_resource
def connect_to_snowflake():
    return create_session()

session = connect_to_snowflake()


# Environment variables below will be automatically populated by Snowflake.
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_STAGE = os.getenv("SNOWFLAKE_STAGE")

# Set the database context
session.sql(f"USE DATABASE {SNOWFLAKE_DATABASE}").collect()
session.sql(f"USE SCHEMA {SNOWFLAKE_SCHEMA}").collect()

# Function to upload file to Snowflake stage
def upload_to_snowflake(file, stage_name):
    with NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(file.read())
        tmp_file_path = tmp_file.name

    try:
        original_file_name = file.name
        actual_file_name = os.path.basename(tmp_file_path)
        # Upload file to Snowflake stage using Snowpark's session.file.put
        session.file.put(f'file://{tmp_file_path}', f'@{stage_name}', auto_compress=False, overwrite=True)
        st.success(f"File '{original_file_name}' uploaded to stage: {stage_name}")
        

        # Need scoped URL for Snowflake staged file
        #scoped_url_df = session.sql(f"SELECT BUILD_SCOPED_FILE_URL(@{stage_name},'/{vendor_folder}/{tmp_file_path}') AS scoped_url")
        #scoped_url = scoped_url_df.collect()[0]['SCOPED_URL']
        #st.write(f"Scoped URL: {scoped_url}")

        # Execute the stored procedure for the vendor
        stored_procedure_name = f"import_prices"
        session.sql(f"CALL {stored_procedure_name}(BUILD_SCOPED_FILE_URL(@{stage_name},'/{actual_file_name}'))").collect()
        st.success(f"Stored procedure for {original_file_name} executed successfully")

    except Exception as e:
        st.error(f"Error uploading file: {str(e)}")


### Now for the Streamlit app

st.markdown("# Excel File Import")

# Upload the file
uploaded_file = st.file_uploader("Choose a file")

if uploaded_file is not None:
    if st.button("Upload"):
        upload_to_snowflake(uploaded_file, SNOWFLAKE_STAGE)

st.markdown("""---""")

# Create a button
st.title("DBT Model Run")

if st.button("Build dbt models"):
    # Trigger the DBT job
    dbt_response = call_dbt_api()
    if dbt_response:
        run_id = dbt_response['data']['id']
        st.write(f"dbt job triggered successfully. Run ID: {run_id}")

        # Check job status periodically
        job_complete = False
        while not job_complete:
            status_response = check_dbt_job_status(run_id)
            if status_response:
                job_status = status_response['data']['is_success']
                if job_status:
                    st.write("dbt job completed successfully.")
                    job_complete = True
                else:
                    st.write("dbt job is still running...")
                    print(job_status)
                    time.sleep(10)  # Wait for 10 seconds before checking status again
            else:
                st.error("Failed to retrieve job status.")
                break

        # Trigger Power BI refresh if DBT job completed successfully
        if job_complete:
            print("Model run completed successfully.")