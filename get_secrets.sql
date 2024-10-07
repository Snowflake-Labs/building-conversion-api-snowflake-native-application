CREATE OR REPLACE PROCEDURE app_schema.SP_GET_SECRETS()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','requests')
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (reference('CONSUMER_EXTERNAL_ACCESS'))
SECRETS = ('api_key' = api_key, 'api_secret_key' = api_secret_key, 'access_token' = access_token, 'access_token_secret' = access_token_secret)
EXECUTE AS OWNER
AS '

import _snowflake
import requests
from json import dumps
import json
import time
import pprint
import time
import datetime
from snowflake.snowpark import Session
import copy
import pandas as pd


def main(snowpark_session:Session):
    secrets_dict = {}

    apikey = _snowflake.get_generic_secret_string("api_key")
    apisecretkey = _snowflake.get_generic_secret_string("api_secret_key")
    accesstoken = _snowflake.get_generic_secret_string("access_token")
    accesstokensecret = _snowflake.get_generic_secret_string("access_token_secret")

    secrets_dict["api_key"] = apikey
    secrets_dict["apisecretkey"] = apisecretkey
    secrets_dict["accesstoken"] = accesstoken
    secrets_dict["accesstokensecret"] = accesstokensecret
    returnstr =  str(secrets_dict).replace("''", ''"'')
    return returnstr

';

GRANT USAGE ON PROCEDURE  app_schema.SP_GET_SECRETS() TO APPLICATION ROLE app_public;
comment on procedure app_schema.SP_GET_SECRETS() is '{"origin":"sf_sit","name":"capi_app","version":{"major":1, "minor":0},"attributes":{"component":"capi_app_demo"}}';