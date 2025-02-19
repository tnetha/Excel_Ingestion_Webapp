# Import python packages
import os
from tempfile import NamedTemporaryFile 
import streamlit as st
from snowflake.snowpark.context import get_active_session
import requests
import time
import _snowflake

# Write directly to the app
st.title("Excel File Upload Demo")

# Get the current credentials
session = get_active_session()


# DBT Cloud API and Power BI API details
DBT_API_URL = 'https://cloud.getdbt.com/api/v2/accounts/<DBT_CLOUD_ACCOUNT_ID>/jobs/<DBT_CLOUD_JOB_ID>/run/'
DBT_JOB_STATUS_URL = 'https://cloud.getdbt.com/api/v2/accounts/<DBT_CLOUD_ACCOUNT_ID>/runs/{run_id}/'
TENANT_ID = _snowflake.get_generic_secret_string('azure_tenant_id')  # Azure AD Tenant ID
CLIENT_ID = '<AZURE_CLIENT_ID>'  # Azure AD Application (client) ID
CLIENT_SECRET = _snowflake.get_generic_secret_string('powerbi_key')  # Client secret for the Azure AD app
RESOURCE = 'https://analysis.windows.net/powerbi/api'  # Power BI resource URL
POWER_BI_API_URL = 'https://api.powerbi.com/v1.0/myorg'  # Power BI API base URL
WORKSPACE_ID = '<POWERBI_WORKSPACE_ID>'  # Power BI workspace ID
DATASET_ID = '<POWERBI_DATASET_ID>'  # Power BI dataset ID

# Trigger DBT Cloud job
def call_dbt_api():
    api_key = _snowflake.get_generic_secret_string('dbt_key')
    headers = {
        'Authorization': f'Token {api_key}'
    }
    body = {
        "cause": "Triggered via API"
    }
    response = requests.post(DBT_API_URL, json=body, headers=headers)

    if response.status_code == 200:
        return response.json()  # Get the run ID from the response
    else:
        st.error("Failed to trigger DBT job.")
        return None
    
# Check DBT Cloud job status
def check_dbt_job_status(run_id):
    api_key = _snowflake.get_generic_secret_string('dbt_key')
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


# Function to upload file to Snowflake stage
def upload_to_snowflake(file, stage_name, vendor_name):
    with NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(file.read())
        tmp_file_path = tmp_file.name

    try:
        original_file_name = file.name
        actual_file_name = os.path.basename(tmp_file_path)
        # Upload file to Snowflake stage using Snowpark's session.file.put
        session.file.put(f'file://{tmp_file_path}', f'@{stage_name}', auto_compress=False, overwrite=True)
        st.success(f"File '{original_file_name}' uploaded to stage: {stage_name}")
        # Execute the stored procedure for the vendor
        stored_procedure_name = f"import_{vendor_name.lower()}_prices"
        session.sql(f"CALL {stored_procedure_name}(BUILD_SCOPED_FILE_URL(@{stage_name},'/{actual_file_name}'))").collect()
        st.success(f"Stored procedure for {original_file_name} executed successfully")

    except Exception as e:
        st.error(f"Error uploading file: {str(e)}")

def get_access_token():
    """
    This function gets the access token from Azure AD using client credentials flow.
    """
    url = f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token'
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    body = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'scope': f'{RESOURCE}/.default'
    }
    
    response = requests.post(url, headers=headers, data=body)
    
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        raise Exception(f"Error obtaining access token: {response.status_code}, {response.text}")

def refresh_dataset(access_token):
    """
    This function triggers a refresh for the specified Power BI dataset.
    """
    url = f'{POWER_BI_API_URL}/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/refreshes'
    
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    
    body = {
        #"notifyOption": "MailOnFailure"  # You can change this to "NoNotification" or "MailOnCompletion"
    }
    
    response = requests.post(url, headers=headers, json=body)
    
    if response.status_code == 202:
        return "Power BI dataset refresh successfully triggered."
    else:
        return f"Error refreshing dataset: {response.status_code}, {response.text}"
        

# Trigger Power BI dataset refresh
def refresh_power_bi_dataset():
    
    # Get the access token
    token = get_access_token()
    
    # Refresh the dataset
    return refresh_dataset(token)


### Now for the Streamlit app

vendor = st.selectbox("Select Vendor", ["Company_A", "Company_B"])

# Upload the file
uploaded_file = st.file_uploader("Choose a file")

if uploaded_file is not None:
    if st.button("Upload"):
        upload_to_snowflake(uploaded_file, "APP_STAGE", vendor)

st.markdown("""---""")

# Create a button
st.title("dbt Model Run")

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
            pbi_response = refresh_power_bi_dataset()
            st.write(pbi_response)
