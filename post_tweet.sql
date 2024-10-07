--Stored Procedure to create a tweet 
CREATE OR REPLACE PROCEDURE app_schema.post_tweet(tweet_text VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
volatile
RUNTIME_VERSION = '3.8'
imports=('/wheel_loader.py',
         '/tweepy-4.14.0-py3-none-any.whl'
        )
PACKAGES = ('snowflake-snowpark-python','pip','simplejson','requests','requests-oauthlib')
SECRETS = ('api_key' = api_key, 'api_secret_key' = api_secret_key, 'access_token' = access_token, 'access_token_secret' = access_token_secret)
HANDLER = 'tweet_function'
EXTERNAL_ACCESS_INTEGRATIONS = (reference('CONSUMER_EXTERNAL_ACCESS'))
AS
$$
import simplejson as json
import wheel_loader
import _snowflake
import requests
from snowflake.snowpark import Session

def tweet_function(snowpark_session:Session,tweet_text):
    # Load Twitter API credentials
    api_key = _snowflake.get_generic_secret_string('api_key')
    api_secret_key = _snowflake.get_generic_secret_string('api_secret_key')
    access_token = _snowflake.get_generic_secret_string('access_token')
    access_token_secret = _snowflake.get_generic_secret_string('access_token_secret')

    # Load external package
    wheel_loader.load('tweepy-4.14.0-py3-none-any.whl')
    call_time = snowpark_session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0]
    try:
        import tweepy
        
        # Set up Tweepy authentication
        try:
            client = tweepy.Client(
                consumer_key=api_key,
                consumer_secret=api_secret_key,
                access_token=access_token,
                access_token_secret=access_token_secret,
                return_type=dict
            )
            
            # Attempt to create tweet
            response = client.create_tweet(text=tweet_text)
            
            
            if response and 'data' in response:
                responsestr = json.dumps(response)
                sql_statement = f"""INSERT INTO app_schema.api_log (api_event_name, api_call_time, call_details, status, response_status_code, response_text) 
                            VALUES ('create_tweet', '{call_time}', '{tweet_text}', 'SUCCESS', '200', '{responsestr}' );"""
                snowpark_session.sql(sql_statement).collect()
                return f"Tweet posted successfully: {response['data']}"
                
            else:
                sql_statement = f"""INSERT INTO app_schema.api_log (api_event_name, api_call_time, call_details, status, response_status_code, response_text) 
                            VALUES ('create_tweet', '{call_time}', '{tweet_text}', 'FAILED', '4XX', '{responsestr}');"""
                snowpark_session.sql(sql_statement).collect()
                return f"Error in response: {response}"
        
        except tweepy.TweepyException as e:
            # Return Tweepy-specific errors
            sql_statement = f"""INSERT INTO app_schema.api_log (api_event_name, api_call_time, call_details, status, response_status_code, response_text) 
                            VALUES ('create_tweet', '{call_time}', '{tweet_text}', 'FAILED', '4XX', '{str(e)}');"""
            snowpark_session.sql(sql_statement).collect()
            return f"Tweepy Error: {str(e)}"
            
        
    except Exception as e:
        # Return general exceptions
        return f"General Error: {str(e)}"
$$;

GRANT USAGE ON PROCEDURE  app_schema.post_tweet(VARCHAR) TO APPLICATION ROLE app_public;
comment on procedure app_schema.post_tweet(VARCHAR) is '{"origin":"sf_sit","name":"capi_app","version":{"major":1, "minor":0},"attributes":{"component":"capi_app_demo"}}';



--Schedule Tweets 

CREATE OR REPLACE PROCEDURE app_schema.POST_TWEET_BATCH()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','pip','simplejson','requests','requests-oauthlib')
HANDLER = 'tweet_function'
imports=('/wheel_loader.py',
         '/tweepy-4.14.0-py3-none-any.whl'
        )
EXTERNAL_ACCESS_INTEGRATIONS = (reference('consumer_external_access'))
SECRETS = ('api_key' = api_key, 'api_secret_key' = api_secret_key, 'access_token' = access_token, 'access_token_secret' = access_token_secret)
EXECUTE AS OWNER
AS '
import simplejson as json
import wheel_loader
import _snowflake
import requests
from snowflake.snowpark import Session
import pandas as pd

def tweet_function(snowpark_session:Session):
    datesql = f"""  select NVL(max(batch_data_end_timestamp), ''1900-01-01''::timestamp_ltz) as startdate , current_timestamp  as enddate, NVL(max(batch_run_id),0)+1 as run_id  from  app_schema.batch_run_detail where batch_id=1;   """
    db_dates= pd.DataFrame(snowpark_session.sql(datesql).collect())
    
    batchstartdate = db_dates.iloc[0][0]
    batchenddate = db_dates.iloc[0][1]
    batchrunid = db_dates.iloc[0][2]
    
    sql_statement = f""" select top 1 tweettext as tweet_text, lastupdatetimestamp from as_db.demo.SAMPLE_TWEET_TABLE  where LastUpdateTimeStamp >''{batchstartdate}''  and LastUpdateTimeStamp <=  ''{batchenddate}'' and record_processed =''N'' ;   """
    #return sql_statement
    tweettext = snowpark_session.sql(sql_statement).collect()
    tweet_text = str(tweettext[0][0])
    tweetlastupdatetimestamp = str(tweettext[0][1])
   
   
   # Load Twitter API credentials
    api_key = _snowflake.get_generic_secret_string(''api_key'')
    api_secret_key = _snowflake.get_generic_secret_string(''api_secret_key'')
    access_token = _snowflake.get_generic_secret_string(''access_token'')
    access_token_secret = _snowflake.get_generic_secret_string(''access_token_secret'')

    # Load external package
    wheel_loader.load(''tweepy-4.14.0-py3-none-any.whl'')
    call_time = snowpark_session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0]
    try:
        import tweepy
        
        # Set up Tweepy authentication
        try:
            client = tweepy.Client(
                consumer_key=api_key,
                consumer_secret=api_secret_key,
                access_token=access_token,
                access_token_secret=access_token_secret,
                return_type=dict
            )
            
            # Attempt to create tweet
            response = client.create_tweet(text=tweet_text)
            
            
            if response and ''data'' in response:
                responsestr = json.dumps(response)
                sql_statement = f"""INSERT INTO app_schema.api_log (api_event_name, api_call_time, call_details, status, response_status_code, response_text) 
                            VALUES (''create_tweet'', ''{call_time}'', ''{tweet_text}'', ''SUCCESS'', ''200'', ''{responsestr}'' );"""
                snowpark_session.sql(sql_statement).collect()
                #return f"Tweet posted successfully: {response[''data'']}"

                sql_statement = f"""UPDATE as_db.demo.SAMPLE_TWEET_TABLE set record_processed =''Y'' where lastupdatetimestamp = 
                                ''{tweetlastupdatetimestamp}'' ;"""
                
                snowpark_session.sql(sql_statement).collect()


                sql_update_batch = f"""INSERT INTO app_schema.batch_run_detail (batch_id, batch_run_id 
                ,batch_record_count,batch_data_start_timestamp, batch_data_end_timestamp) 
                               SELECT 1, ''{batchrunid}'',1, ''{batchstartdate}'', ''{batchenddate}'' ; """
                snowpark_session.sql(sql_update_batch).collect()
        

                
                return f"Tweet posted successfully: {response[''data'']}"
                
            else:
                sql_statement = f"""INSERT INTO app_schema.api_log (api_event_name, api_call_time, call_details, status, response_status_code, response_text) 
                            VALUES (''create_tweet'', ''{call_time}'', ''{tweet_text}'', ''FAILED'', ''4XX'', ''{responsestr}'');"""
                snowpark_session.sql(sql_statement).collect()
                return f"Error in response: {response}"
        
        except tweepy.TweepyException as e:
            # Return Tweepy-specific errors
            sql_statement = f"""INSERT INTO app_schema.api_log (api_event_name, api_call_time, call_details, status, response_status_code, response_text) 
                            VALUES (''create_tweet'', ''{call_time}'', ''{tweet_text}'', ''FAILED'', ''4XX'', ''{str(e)}'');"""
            snowpark_session.sql(sql_statement).collect()
            return f"Tweepy Error: {str(e)}"
            
        
    except Exception as e:
        # Return general exceptions
        return f"General Error: {str(e)}"
';


GRANT USAGE ON PROCEDURE  app_schema.POST_TWEET_BATCH() TO APPLICATION ROLE app_public;
comment on procedure app_schema.POST_TWEET_BATCH() is '{"origin":"sf_sit","name":"capi_app","version":{"major":1, "minor":0},"attributes":{"component":"capi_app_demo"}}';




