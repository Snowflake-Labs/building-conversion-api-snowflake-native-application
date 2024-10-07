import streamlit as st
import datetime
from commons import CommonObjects 


st.set_page_config(
    page_title="Capi-API DEMO",
    page_icon="Logo.png",
    layout="wide",
    initial_sidebar_state="expanded"
    )

CommonObjects.header()
st.header("Conversion API (CAPI) ❄️ App - DEMO")
st.write("""
This Native Application is using Snowflake EAI, Streamlit and Native Apps as the framework for the user experience.  Streamlit is an open-source app framework built specifically for Rapid Application Development and Data Science projects.
""")

st.header("Troubleshooting")

st.write("1️⃣ Visit the Configuration page and check and/or set your secrets")
st.write("2️⃣ Visit the API Testing page and check to see if your secrets work by trying the API Tests")
st.write("3️⃣ Visit the Operations page and review the log data, discuss concerns with your TAM and download/share your logs with them if needed")


st.header("The X Developer API Documentation")
st.markdown("""
This application uses API Endpoints from The X

    (https://developer.x.com/en/docs)

""")


st.header("Learn more about Streamlit and/or Snowflake Native Apps")
st.markdown("""
- Check out: [streamlit.io](https://streamlit.io)
- Check out: [Snowflake Native Apps](https://www.snowflake.com/en/data-cloud/workloads/applications/native-apps/)
""")

st.header("Credentials/Secrets Security")
st.markdown("""
Credentials are Stored in Snowflake as **Secrets**.  A Snowflake secret is a schema-level object that stores sensitive information, limits access to the sensitive information using RBAC, and is encrypted using the Snowflake key encryption hierarchy. Information present in the secret object is encrypted using a key in the key hierarchy. After you create a secret, only dedicated Snowflake components such as integrations and external functions can read the sensitive information.
\nLearn more about Secrets here: [Snowflake Secrets](https://docs.snowflake.com/en/user-guide/api-authentication)
""")


CommonObjects.footer()