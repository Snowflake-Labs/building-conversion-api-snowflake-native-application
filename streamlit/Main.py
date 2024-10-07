import streamlit as st
import datetime
import sys
from snowflake.snowpark.context import get_active_session
from commons import CommonObjects



def InitializeApp():
    response = session.call("app_schema.init_app")
    if isinstance(response, str) and "Initialization complete" in response:
        st.info(response)
    else:
        response = "Error : " + response





st.set_page_config(
    page_title="Demo-Capi-API",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

session = get_active_session()
CommonObjects.header()

st.title("Conversion API (CAPI) App - DEMO ")

st.caption(
    """
    This demo app showcases a use case of calling an external API with test payloads as well as batched payloads. An application of such capabilities can be seen in COnversion API's where a consumer calls API's to provide conversion data to the provider/ 
"""
)

st.subheader("Please follow these steps")
steps_cols = st.columns(6)
with steps_cols[0]:
    st.title("$~~~~$:blue[①]", anchor=False)
    st.subheader("Initialize App", anchor=False)
    st.markdown("Initialize App to create the required methods that perform external access")
    st.button(
    "Initialize ",
    on_click=InitializeApp,
    type="primary"
)

with steps_cols[1]:
    st.title("$~~~~$:blue[②]", anchor=False)
    st.subheader("Configure App", anchor=False)
    st.markdown("Review the Configuration page to check if all secrets for external access have been created")

with steps_cols[2]:
    st.title("$~~~~$:blue[③]", anchor=False)
    st.subheader("Test API connection", anchor=False)
    st.markdown("Use a text string to call the X-API for creating a tweet. ")

with steps_cols[3]:
    st.title("$~~~~$:blue[④]", anchor=False)
    st.subheader("Monitor Operations", anchor=False)
    st.markdown(
        "Monitor the Operations page and review the log data and operational metrics"
    )

with steps_cols[4]:
    st.title("$~~~~$:blue[⑤]", anchor=False)
    st.subheader("Batch Records", anchor=False)
    st.markdown(
        "Review the batching metadata tables and data"
    )

with steps_cols[5]:
    st.title("$~~~~$:blue[⑤]", anchor=False)
    st.subheader("Schedule Batch API calls", anchor=False)
    st.markdown(
        "Create tasks to call the Batch processes to call API's"
    )

st.divider()

CommonObjects.footer()