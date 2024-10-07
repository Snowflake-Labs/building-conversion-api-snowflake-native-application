-- Author: Ajita
-- Email: ajita.sharma@snowflake.com
-- Date Created: 8/14/24


-- Create the application package
CREATE APPLICATION PACKAGE capi_app_demo_pkg;

-- Verify the application package was successfully created
SHOW APPLICATION PACKAGES;

-- Set the context to the application package
USE APPLICATION PACKAGE capi_app_demo_pkg;

-- Create the required schema for the named stage
CREATE SCHEMA stage_content;
CREATE SCHEMA shared_content;

-- Create the named stage
CREATE OR REPLACE STAGE capi_app_demo_pkg.stage_content.capi_stage
    FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER = '|' SKIP_HEADER = 1);

-- Verify the files were successfully uploaded
LIST @capi_app_demo_pkg.stage_content.capi_stage;


CREATE VIEW capi_app_demo_pkg.SHARED_CONTENT.ACCOUNT_SECRETS
  AS select account_id,api_key,api_secret_key,access_token,access_token_secret   from  capi_consumer_accounts.sec.secrets ;


 
  
  
 ------------GRANT REFRENCE USAGE --------
 GRANT USAGE ON SCHEMA capi_app_demo_pkg.shared_content
  TO SHARE IN APPLICATION PACKAGE capi_app_demo_pkg;
 
 GRANT REFERENCE_USAGE ON DATABASE capi_consumer_accounts
  TO SHARE IN APPLICATION PACKAGE capi_app_demo_pkg;

 GRANT SELECT ON VIEW capi_app_demo_pkg.SHARED_CONTENT.ACCOUNT_SECRETS
  TO SHARE IN APPLICATION PACKAGE capi_app_demo_pkg;



  ---------------------------------------------------------------------------------------

-- View the versions and patches defined for the application package
SHOW VERSIONS IN APPLICATION PACKAGE capi_app_demo_pkg;

-- Set the default release direction to version v1_0 and patch 0
ALTER APPLICATION PACKAGE capi_app_demo_pkg
  ADD VERSION v1_0 USING '@capi_app_demo_pkg.stage_content.capi_stage';



-- View the versions and patches defined for the application package
SHOW VERSIONS IN APPLICATION PACKAGE capi_app_demo_pkg;

-- Set the default release direction to version v1_0 and patch 0
ALTER APPLICATION PACKAGE capi_app_demo_pkg
  SET DEFAULT RELEASE DIRECTIVE
  VERSION = v1_0
  PATCH = 0;

