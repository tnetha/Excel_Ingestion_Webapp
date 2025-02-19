use role accountadmin;
use database app_db;
use schema app_schema;
use warehouse app_warehouse;
-- Set up network rule
CREATE OR REPLACE NETWORK RULE dbtcloud_apis_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('cloud.getdbt.com');


DROP SECRET dbt_cloud_api;

CREATE OR REPLACE SECRET dbt_cloud_api
  TYPE = GENERIC_STRING
  SECRET_STRING = '';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbt_cloud_api_access_integration
  ALLOWED_NETWORK_RULES = (dbtcloud_apis_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (dbt_cloud_api)
  ENABLED = true;

GRANT USAGE ON INTEGRATION dbt_cloud_api_access_integration to role app_role;
GRANT USAGE ON SECRET dbt_cloud_api to role app_role;


-- Now create Power BI API integration

CREATE OR REPLACE NETWORK RULE powerbi_apis_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('login.microsoftonline.com', 'analysis.windows.net', 'api.powerbi.com');

CREATE OR REPLACE SECRET powerbi_api
  TYPE = GENERIC_STRING
  SECRET_STRING = '';

CREATE OR REPLACE SECRET azure_tenant_id
  TYPE = GENERIC_STRING
  SECRET_STRING = '';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION powerbi_api_access_integration
  ALLOWED_NETWORK_RULES = (powerbi_apis_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (powerbi_api, azure_tenant_id)
  ENABLED = true;

GRANT USAGE ON INTEGRATION powerbi_api_access_integration to role app_role;

SHOW STREAMLITS;

ALTER STREAMLIT app_db.app_schema.<INSERT_STREAMLIT_ID>
  SET EXTERNAL_ACCESS_INTEGRATIONS = (dbt_cloud_api_access_integration, powerbi_api_access_integration);
  SECRETS = ('dbt_key' = app_db.app_schema.dbt_cloud_api, 'powerbi_key' = app_db.app_schema.powerbi_api, 'azure_tenant_id' = app_db.app_schema.azure_tenant_id);