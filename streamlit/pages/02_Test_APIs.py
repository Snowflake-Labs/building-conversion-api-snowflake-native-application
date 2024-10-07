import streamlit as st
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as f
from commons import CommonObjects
import pandas as pd

st.set_page_config(
    page_title="Capi-API Demo",
    page_icon="Logo.png",
    layout="wide",
    initial_sidebar_state="expanded",
)




def createtweet(tweettext):
    response = session.call("app_schema.post_tweet", tweettext)
    if isinstance(response, str) and "Tweet posted successfully: {" in response:
        st.info(response)
    else:
        response = "Error : " + response
       


CommonObjects.header()

session = get_active_session()

st.header("TEST CREATE TWEET API CALL  !! ")
st.divider()
st.subheader("Create New Tweet with a sample string ")
tweettext = st.text_input("Tweet Text", placeholder="Enter the tweet text")
st.button(
    "Create Tweet ",
    on_click=createtweet,
    args=[tweettext],
    disabled=not tweettext,
    type="primary" if tweettext else "secondary",
)

CommonObjects.footer()