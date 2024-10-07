SYSTEM$LOG('error', 'Error message');
CREATE APPLICATION ROLE app_public;
CREATE OR ALTER VERSIONED SCHEMA app_schema;
GRANT USAGE ON SCHEMA app_schema TO APPLICATION ROLE app_public;



CREATE OR REPLACE SEQUENCE app_schema.batch_id_seq;
CREATE OR REPLACE SEQUENCE app_schema.batch_run_id_seq;


CREATE OR REPLACE STREAMLIT app_schema.CAPIDemoApp
  FROM '/streamlit'
  MAIN_FILE = '/Main.py'
;
GRANT USAGE ON STREAMLIT app_schema.CAPIDemoApp TO APPLICATION ROLE app_public;



--CREATE API LOG TABLE

create or replace TABLE app_schema.API_LOG (
	API_EVENT_NAME VARCHAR(16777216) COMMENT 'custom api event name',
	API_CALL_TIME TIMESTAMP_LTZ(9) COMMENT 'timestamp of API call',
	CALL_DETAILS VARCHAR(16777216) COMMENT 'details on the request for API call',
	STATUS VARCHAR(16777216) COMMENT 'decoded value of response code Success,Failed, Warning',
	RESPONSE_STATUS_CODE VARCHAR(16777216) COMMENT 'https response code',
	RESPONSE_TEXT VARCHAR(16777216) COMMENT 'https response text'
);
GRANT SELECT , INSERT, DELETE  ON TABLE app_schema.API_LOG to APPLICATION ROLE app_public;
comment on table app_schema.API_LOG is '{"origin":"sf_sit","name":"capi_app","version":{"major":1, "minor":0},"attributes":{"component":"capi_app_demo"}}';



--Create metadata tables for batching process

create or replace table app_schema.BATCH_MAIN(
                    batch_id          int           DEFAULT app_schema.batch_id_seq.nextval    comment 'unique batch identifier',
                    batch_event_name  varchar                                       comment 'name of the stored proc that calls the event API ',
                    batch_parameters  varchar                                       comment 'list of parameters names for the API sp call ex. adverstiserID, providerID',
                    is_on_demand      char                                          comment 'flag to indicate if this is a scheduled event. Setup events are set to N ',
                    batch_source_objects  varchar                                   comment 'Source tables or views records for creating the API payload ',
                    batch_size        int                                           comment 'Number of records to pick for the payload',
                    batch_task_name   varchar                                       comment ' Name of the task that runs this batch',
                    batch_task_schedule   varchar                                   comment ' Batch runs schedule'
                          );

GRANT SELECT , INSERT, DELETE  ON TABLE app_schema.BATCH_MAIN to APPLICATION ROLE app_public;
comment on table app_schema.BATCH_MAIN is '{"origin":"sf_sit","name":"capi_app","version":{"major":1, "minor":0},"attributes":{"component":"capi_app_demo"}}';

insert into app_schema.batch_main ( batch_id, batch_event_name, batch_parameters, is_on_demand, batch_source_objects,batch_size, batch_task_name, batch_task_schedule  )
select 1, 'CREATE_TWEET','SAMPLE_TWEET_TABLE','N',null,null,'CREATE_TWEET_TASK','EVERY 5 MINS';

create or replace table app_schema.batch_run_detail( 
                         batch_id                    int                                                comment 'unique batch identifier',
                         batch_run_id                int            DEFAULT app_schema.batch_run_id_seq.nextval    comment 'Unique ID for a run ',
                         batch_record_count          int                                                comment 'Number of records in the batch',
                         batch_data_start_timestamp  timestamp_ltz                                      comment 'Start timestamp for SQL to pick records from the source tables for creating the API payload ',
                         batch_data_end_timestamp   timestamp_ltz                                       comment 'End timestamp for SQL to pick records from the source tables for creating the API payload' );


GRANT SELECT , INSERT, DELETE  ON TABLE app_schema.batch_run_detail to APPLICATION ROLE app_public;
comment on table app_schema.batch_run_detail is '{"origin":"sf_sit","name":"capi_app","version":{"major":1, "minor":0},"attributes":{"component":"capi_app_demo"}}';


--Create a data table that will be used for creating payloads for calling the API


  create or replace TABLE app_schema.SAMPLE_TWEET_TABLE (
	TweetID INTEGER,
    TweetDate TIMESTAMP_LTZ(9),
	TweetText VARCHAR(16777216),
    LastUpdateTimeStamp TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)),
	RECORD_PROCESSED CHAR(1)
    );

Insert into app_schema.sample_tweet_table(tweetId, tweetDate , tweettext, RECORD_PROCESSED ) select 1,'01/01/2024','Hello, This is my first tweet' ,'N' ;
Insert into app_schema.sample_tweet_table(tweetId, tweetDate , tweettext ,RECORD_PROCESSED)  select 2,'01/02/2024','Hello, This is my second tweet' ,'N' ;
Insert into app_schema.sample_tweet_table(tweetId, tweetDate , tweettext, RECORD_PROCESSED)  select 3,'01/03/2024','Hello, This is my third tweet' ,'N' ;
Insert into app_schema.sample_tweet_table(tweetId, tweetDate , tweettext, RECORD_PROCESSED)  select 4,'01/04/2024','Hello, This is my fourth tweet' ,'N';

GRANT SELECT , INSERT, DELETE  ON TABLE app_schema.SAMPLE_TWEET_TABLE to APPLICATION ROLE app_public;
comment on table app_schema.SAMPLE_TWEET_TABLE is '{"origin":"sf_sit","name":"capi_app","version":{"major":1, "minor":0},"attributes":{"component":"capi_app_demo"}}';





CREATE or replace PROCEDURE app_schema.create_secrets()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$
var message = "Started Prc: ";
try {


 var currentaccountsql = "SELECT CURRENT_ACCOUNT();"
 var query = snowflake.createStatement({sqlText: currentaccountsql});
 var result = query.execute();
 
  while (result.next())
    {
         currentaccountid  =  result.getColumnValue(1).trim();
	}
  message = message +"currentaccount:" +currentaccountid;
 
  if (currentaccountid.trim() != '') 
  {
   var sqlgetsecretvalues = "select ACCOUNT_ID,API_KEY,api_secret_key,access_token,access_token_secret from SHARED_CONTENT.ACCOUNT_SECRETS where account_id = '"+ currentaccountid +"' ;";
   message = message+"sql:"+sqlgetsecretvalues
   var stmt = snowflake.createStatement({sqlText: sqlgetsecretvalues});
   var resultSet = stmt.execute();
   recordcount = resultSet.getRowCount()
   message = message+ "recordcount:"+recordcount
  
  }
  
   if (resultSet.getRowCount() <= 0) 
    {
        return 'error'
    }
  recordcount = resultSet.getRowCount() 
  message = message+"Entering while and record count: " + recordcount
  while (resultSet.next())
    {
         accountid =  resultSet.getColumnValue(1).trim();
         api_key = resultSet.getColumnValue(2).trim();
         api_secret_key = resultSet.getColumnValue(3).trim();
         access_token = resultSet.getColumnValue(4).trim();
         access_token_secret = resultSet.getColumnValue(5).trim();
         

         
		 
         var create_apikey_secretsql = "CREATE OR REPLACE SECRET app_schema.api_key TYPE = GENERIC_STRING SECRET_STRING = '"+api_key+"';"
         message = message +"sqk:" +create_apikey_secretsql ;
         snowflake.execute ({sqlText: create_apikey_secretsql});
         snowflake.execute({sqlText: "GRANT USAGE ON SECRET app_schema.api_key to APPLICATION ROLE app_public; "});
         
         var create_apisecretkey_secretsql = "CREATE OR REPLACE SECRET app_schema.api_secret_key TYPE = GENERIC_STRING SECRET_STRING = '"+api_secret_key+"';"
         snowflake.execute ({sqlText: create_apisecretkey_secretsql});
         snowflake.execute({sqlText: "GRANT USAGE ON SECRET app_schema.api_secret_key to APPLICATION ROLE app_public; "}); 
         
         var create_access_token_secretsql = "CREATE OR REPLACE SECRET app_schema.access_token TYPE = GENERIC_STRING SECRET_STRING = '"+access_token+"';"
         snowflake.execute ({sqlText: create_access_token_secretsql});
         snowflake.execute({sqlText: "GRANT USAGE ON SECRET app_schema.access_token to APPLICATION ROLE app_public; "}); 
         
         var creates_access_token_secret_secretsql = "CREATE OR REPLACE SECRET app_schema.access_token_secret TYPE = GENERIC_STRING SECRET_STRING = '"+access_token_secret+"';"
         snowflake.execute ({sqlText: creates_access_token_secret_secretsql});
         snowflake.execute({sqlText: "GRANT USAGE ON SECRET app_schema.access_token_secret to APPLICATION ROLE app_public; "}); 
         
        

        
    }
}

 catch (err)  {
      result =  "Failed: Code: " + err.code + "\\n  State: " + err.state;
      result += "\\n  Message: " + err.message;
      result += "\\nStack Trace:\\n" + err.stackTraceTxt;
      result += "message: " + message
      return result;
      }

  return 'SUCCESS CREATING SECRETS';
$$;

GRANT USAGE ON PROCEDURE app_schema.create_secrets() TO APPLICATION ROLE app_public;
comment on procedure app_schema.create_secrets() is '{"origin":"sf_sit","name":"capi_app","version":{"major":1, "minor":0},"attributes":{"component":"capi_app_demo"}}';
call app_schema.create_secrets();





--create register_callback function
CREATE OR REPLACE PROCEDURE app_schema.REGISTER_SINGLE_CALLBACK(ref_name string, operation string, ref_or_alias string)
RETURNS STRING
LANGUAGE SQL
COMMENT = '{"origin":"sf_sit","name":"capi_app","version":{"major":1, "minor":5},"attributes":{"role":"provider","component":"register_single_callback"}}'
AS 
$$
  BEGIN
  CASE (operation)
   WHEN 'ADD' THEN
      SELECT SYSTEM$ADD_REFERENCE(:ref_name, :ref_or_alias);
   WHEN 'REMOVE' THEN
      SELECT SYSTEM$REMOVE_REFERENCE(:ref_name, :ref_or_alias);
   WHEN 'CLEAR' THEN
      SELECT SYSTEM$REMOVE_ALL_REFERENCES(:ref_name);
   ELSE
      RETURN 'unknown operation: ' || operation;
  END CASE;
  RETURN 'operation: ' || operation || ' for alias: ' || ref_or_alias || ' complete.';
  END;
$$;

GRANT USAGE ON PROCEDURE app_schema.REGISTER_SINGLE_CALLBACK(STRING,STRING,STRING) TO APPLICATION ROLE app_public;
comment on procedure app_schema.REGISTER_SINGLE_CALLBACK(STRING,STRING,STRING) is '{"origin":"sf_sit","name":"capi_app","version":{"major":1, "minor":0},"attributes":{"component":"capi_app_demo"}}';




create or replace procedure app_schema.get_configuration_for_reference(ref_name string)
returns string
language sql
as $$
begin
    
    case (ref_name)
    when 'CONSUMER_EXTERNAL_ACCESS' then
        return '{
        "type": "CONFIGURATION",
        "payload": {
            "host_ports": ["api.twitter.com"],
			"allowed_secrets" : "ALL"
           }
        }';
    end case;
    return '{
        "type": "ERROR",
        "payload":{
            "message": "The reference is not available for configuration ..."
        }
        }';
end;
$$;




GRANT USAGE ON PROCEDURE app_schema.get_configuration_for_reference(string) TO APPLICATION ROLE app_public;
comment on procedure app_schema.get_configuration_for_reference(string) is '{"origin":"sf_sit","name":"capi_app","version":{"major":1, "minor":0},"attributes":{"component":"capi_app_demo"}}';



CREATE OR REPLACE PROCEDURE app_schema.init_app()
returns string
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'create_sps'
AS
$$
import os
def create_sps(session):
  try:
    files = ['get_secrets.sql','post_tweet.sql']
    for f in files:
      create_sp(session,'/' + f)
    return "Initialization complete"
  except Exception as ex:
        return ex
        raise ex

def create_sp(session, filename):
    file = session.file.get_stream(filename)
    create_sp_ddl = file.read(-1).decode("utf-8")
    session.sql("begin " + create_sp_ddl + " end;").collect()
    return f'{filename} created'    
$$;
GRANT USAGE ON PROCEDURE app_schema.init_app() to application role app_public;
comment on procedure app_schema.init_app() is '{"origin":"sf_sit","name":"capi_app","version":{"major":1, "minor":0},"attributes":{"component":"capi_app_demo"}}';


create schema app_orchestration ;
grant usage on schema app_orchestration to APPLICATION ROLE app_public;


CREATE or replace PROCEDURE app_orchestration.create_task(WH_NAME varchar, schedule varchar, taskname varchar, spname varchar)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$


var create_task_cmd = "create task if not exists app_orchestration." + TASKNAME  + " SCHEDULE = '"+ SCHEDULE +"' USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'  as call "+ SPNAME +"() ;";
snowflake.execute({ sqlText: create_task_cmd });
var resumetasksql = "alter task app_orchestration."+ TASKNAME +" resume ;";
snowflake.execute({ sqlText: resumetasksql });
return "SERVICE STARTED";

$$;

GRANT USAGE ON PROCEDURE  app_orchestration.create_task(VARCHAR,VARCHAR,VARCHAR,VARCHAR) TO APPLICATION ROLE app_public;
comment on procedure  app_orchestration.create_task(VARCHAR,VARCHAR,VARCHAR,VARCHAR) is '{"origin":"sf_sit","name":"capi_app","version":{"major":1, "minor":0},"attributes":{"component":"capi_app_demo"}}';

CREATE OR REPLACE PROCEDURE app_orchestration.update_task(taskname varchar, action varchar)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$
var action = ACTION
if ( action == 'SUSPEND' )  
  {
  var sql = " ALTER TASK IF EXISTS app_orchestration." + TASKNAME + " SUSPEND;";
  var sqlupdate = " UPDATE app_orchestration.SCHEDULES SET status = 'SUSPENDED' where SCHEDULE_NAME = '" + TASKNAME + "';";
  snowflake.execute({ sqlText: sql });
 
  snowflake.execute({ sqlText: sqlupdate });
  return "SUSPENDED";
  }
if ( action == 'RESUME')
{
  var sql = " ALTER TASK IF EXISTS app_orchestration." + TASKNAME + " RESUME;";
  snowflake.execute({ sqlText: sql });
  var sqlupdate = " UPDATE app_orchestration.SCHEDULES SET status = 'ACTIVE' where SCHEDULE_NAME = '" + TASKNAME + "';";
  snowflake.execute({ sqlText: sqlupdate });
  return "RESUMED !";
}

if ( action =='DROP')
{
  var sql = " DROP TASK if exists app_orchestration." + TASKNAME + " ;";
  snowflake.execute({ sqlText: sql });
  var sqlupdate = " DELETE FROM app_orchestration.SCHEDULES  where SCHEDULE_NAME = '" + TASKNAME + "';"
  snowflake.execute({ sqlText: sqlupdate });
  return "TASK DELETED ";

}

$$;

GRANT USAGE ON PROCEDURE  app_orchestration.update_task(VARCHAR,VARCHAR) TO APPLICATION ROLE app_public;
comment on procedure  app_orchestration.update_task(VARCHAR,VARCHAR) is '{"origin":"sf_sit","name":"capi_app","version":{"major":1, "minor":0},"attributes":{"component":"capi_app_demo"}}';