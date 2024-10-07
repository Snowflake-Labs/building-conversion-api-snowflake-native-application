from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import streamlit as st
import datetime
import base64
from PIL import Image
import utils as u

LOCAL = True


class SiSConnect:
    def connect() -> Session:
        if LOCAL:
            session = Session.builder.configs(
                st.secrets.get("connections").get("blue")
            ).create()
        else:
            session = get_active_session()
        return session


class CommonObjects:

    def header():

        logo = u.return_image("Logo.png")
        st.image(logo, width=500)
        

    def footer():
        ver = str(st.__version__)
        yr = datetime.datetime.now().strftime("%Y")
        footer = f"""
        ©{yr} Snowflake . All rights reserved.
        Application written in Snowflake Streamlit version {ver}
        """
        st.code(footer, language="None")