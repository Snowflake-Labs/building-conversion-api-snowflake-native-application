import streamlit as st
import datetime
from snowflake.snowpark import Session
import sys



from commons import CommonObjects


# import _snowflake
import pandas as pd
from snowflake.snowpark.context import get_active_session
import json
import snowflake.snowpark.exceptions


st.set_page_config(
    page_title="Capi-API Demo",
    page_icon="Logo.png",
    layout="wide",
    initial_sidebar_state="expanded",
)


session = get_active_session()
CommonObjects.header()


global secrets_str
with st.spinner("Getting Secrets"):
    secrets_str = session.call("app_schema.SP_GET_SECRETS")
    my_dict = json.loads(secrets_str)
    api_key = my_dict.get("api_key")
    apisecretkey = my_dict.get("apisecretkey")
    accesstoken = my_dict.get("accesstoken")
    accesstokensecret = my_dict.get("accesstokensecret")
    

config_colunms = st.columns(2, gap="large")
with config_colunms[0]:
    st.subheader(
        ":blue[CAPI App API Secrets]",
        help="""
🟢 Indicates the API Secret is already saved within the App
\n🔴 Indicates the API Secret is not yet saved within the App
""",
    )
    if api_key is not None:
        st.success(f"**Auth API key **${'~'*20}$|$~~${'●'*10}", icon="✅")
    else:
        st.error(f"Reset API Auth key$~~$**API Auth key**$~~${'●'*10}", icon="⚠️")
    if apisecretkey is not None:
        st.success(f"**API secret key **${'~'*21}$|$~~${'●'*10}", icon="✅")
    else:
        st.error(f"**apisecretkey**", icon="⚠️")

    if accesstoken is not None:
        st.success(
            f"**Access Token **${'~'*6}$|$~~${'●'*10}",
            icon="✅",
        )
    else:
        st.error(f"**Access Token **", icon="⚠️")
    if accesstokensecret is not None:
        st.success(f"**Access token secret **${'~'*29}$|$~~${'●'*10}", icon="✅")
    else:
        st.error(f"**Access token secret **", icon="⚠️")
  

CommonObjects.footer()