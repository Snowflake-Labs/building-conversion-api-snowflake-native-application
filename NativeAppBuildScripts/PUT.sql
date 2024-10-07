-- Sample PUT commands to upload files to a stage, source file path will have to be updated in the below commands

PUT file:////Users/Dev/X-API/CAPI-Demo/*.*   @capi_app_demo_pkg.stage_content.capi_stage overwrite=true auto_compress=false;   

PUT file:////Users/Dev/X-API/CAPI-Demo/streamlit/*.*   @capi_app_demo_pkg.stage_content.capi_stage/streamlit/  overwrite=true auto_compress=false; 

PUT file:////Users/Dev/X-API/CAPI-Demo/streamlit/pages/*.*  @capi_app_demo_pkg.stage_content.capi_stage/streamlit/pages/  overwrite=true auto_compress=false;   
 
