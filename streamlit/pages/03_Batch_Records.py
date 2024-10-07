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


def alternating_rows(val):
    return "background-color: #90cde8; "


def split_frame(input_df, rows):
    df = [input_df.loc[i : i + rows - 1, :] for i in range(0, len(input_df), rows)]
    return df


def displayTweetTable():

    wh_table = (
        session.table("app_schema.sample_tweet_table")
        .select(
            f.col('tweetId').alias("Tweet ID"),
            f.col('tweetDate').alias("Tweet Date"),
            f.col('tweettext').alias("Tweet Text"),
            f.col('RECORD_PROCESSED').alias("Tweet Posted ?"),
        )
        .to_pandas()
    )
    if wh_table.shape[0] > 25:

        batch_size = pagination_menu[2].radio(
            "Page Size", options=[25, 50, 100], horizontal=True
        )

        total_pages = (
            int(wh_table.shape[0] / batch_size)
            if int(wh_table.shape[0] / batch_size) > 0
            else 1
        )
        current_page = pagination_menu[1].number_input(
            "Page", min_value=1, max_value=total_pages, step=1
        )

        pages = split_frame(wh_table, batch_size)
        current_frame = pd.DataFrame(pages[current_page - 1])
        st.dataframe(
            data=current_frame.style.apply(
                lambda _: current_frame.iloc[::2].applymap(alternating_rows),
                axis=None,
            ),
            use_container_width=True,
        )
        st.caption(
            f"Page **{current_page}** of **{total_pages}** ",
        )
    else:
        st.dataframe(
            wh_table.style.apply(
                lambda _: wh_table.iloc[::2].applymap(alternating_rows),
                axis=None,
            ),
            use_container_width=True,
        )


CommonObjects.header()
pagination_menu = st.columns((4, 0.75, 1))
pagination_menu[0].header("Batching Records")
session = get_active_session()


st.header("Sample tweet table for creating batched payloads for API ")
TweetTable = session.table("app_schema.sample_tweet_table").select(
            f.col('tweetId').alias("TweetID"),
            f.col('tweetDate').alias("TweetDate"),
            f.col('tweettext').alias("TweetText"),
            f.col('RECORD_PROCESSED').alias("TweetPosted ?"),
        ) 
st.dataframe(TweetTable)
st.divider()

st.header("Batch metdata definition table ")

BatchMain = session.table("app_schema.batch_main").select(
            f.col('batch_id').alias("BatchID"),
            f.col('batch_event_name').alias("BatchName"),
            f.col('batch_parameters').alias("BatchParameters"),
            f.col('batch_source_objects').alias("BatchRecordSource"),
        ).to_pandas()
st.dataframe(BatchMain)
st.divider()
st.header("Batch run metadata table  ")

BatchRunDetails = session.table("app_schema.batch_run_detail").select(
                         f.col('batch_id').alias("BatchID") ,
                         f.col('batch_run_id').alias("BatchRunID") ,
                         f.col('batch_record_count').alias("RecordCount") ,
                         f.col('batch_data_start_timestamp').alias("BatchDataStartTimeStamp") ,
                         f.col('batch_data_end_timestamp').alias("BatchDataEndTimeStamp"),).to_pandas()
st.dataframe(BatchRunDetails)  

CommonObjects.footer()