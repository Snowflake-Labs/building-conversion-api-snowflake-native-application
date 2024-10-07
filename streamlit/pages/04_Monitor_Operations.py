# Import python packages
import streamlit as st
import pandas as pd
import datetime
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import functions as f
from commons import CommonObjects, SiSConnect
import sys
import altair as alt

st.set_page_config(
    page_title="Capi-API Demo",
    page_icon="Logo.png",
    layout="wide",
    initial_sidebar_state="expanded",
)

CommonObjects.header()


@st.cache_data
def convert_df(df):
    return df.to_csv().encode("utf-8")


def alternating_rows(val):
    return "background-color: #E6EDD6;"


# emoji heaven
# https://streamlit-emoji-shortcodes-streamlit-app-gwckff.streamlit.app/
# ❌ ❓❗✅ ❔ ✔️ ✖️	 🔴 🟢

st.header("API Operations Dashboard")

st.write(
    """
    See below for operational logging information
    """
)


session = get_active_session()

@st.cache_data(show_spinner=False)
def get_logs() -> pd.DataFrame:
    log_table = """with event_range as 
                    (
                    select dateadd(day, seq4()*-1, current_date()) as AGGREGATE_DATE from table(generator(rowcount=>14))
                    ) 

                    select LD.AGGREGATE_DATE, COUNT(STATUS) AS EVENT_COUNT, STATUS, RESPONSE_STATUS_CODE, api_event_name from event_range LD
                    LEFT JOIN app_schema.API_LOG AL ON LD.AGGREGATE_DATE = AL.API_CALL_TIME::DATE
                    GROUP BY ALL"""
    log_data = session.sql(log_table)
    return log_data.to_pandas()


statuses = ["SUCCESS", "FAILED", "NO CALL"]
color = ["GREEN", "RED", "ORANGE"]
raw_log_data = get_logs()

selection = alt.selection_interval(encodings=["x"])
log_chart_base = (
    alt.Chart(raw_log_data)
    .mark_line(color="GREEN", size=2)
    .encode(
        x=alt.X(
            "AGGREGATE_DATE",
            type="temporal",
            timeUnit="yearmonthdate",
            title="Date",
            axis=alt.Axis(
                ticks=True,
                labels=True,
            ),
        ),
        y=alt.Y(
            "EVENT_COUNT",
            type="quantitative",
            title="API Call Count",
            aggregate="sum",
            axis=alt.Axis(tickMinStep=1, labelAlign="center"),
        ),
    )
    .properties(title="API Events 14 Days View")
    .add_selection(selection)
)

log_chart_days = (
    alt.Chart(raw_log_data)
    .mark_point(size=150, color="red", filled=True)
    .encode(
        x=alt.X(
            "AGGREGATE_DATE",
            type="temporal",
            timeUnit="yearmonthdate",
        ),
        y=alt.Y(
            "EVENT_COUNT",
            type="quantitative",
            aggregate="sum",
            axis=alt.Axis(tickMinStep=1, labelAlign="center"),
        ),
        tooltip=[
            alt.Tooltip("EVENT_COUNT", title="Failed API Calls"),
            alt.Tooltip("API_EVENT_NAME", title="API"),
        ],
    )
    .transform_filter(alt.datum.STATUS == "FAILED")
    .properties()
)

log_chart_no_events = (
    alt.Chart(raw_log_data)
    .mark_point(size=100, color="orange")
    .encode(
        x=alt.X(
            "AGGREGATE_DATE",
            type="temporal",
            timeUnit="yearmonthdate",
        ),
        y=alt.Y(
            "EVENT_COUNT",
            type="quantitative",
            aggregate="sum",
            axis=alt.Axis(tickMinStep=1, labelAlign="center"),
        ),
        tooltip=[alt.Tooltip("EVENT_COUNT", title="No API Calls Found")],
    )
    .transform_filter(alt.datum.EVENT_COUNT == 0)
)

log_chart_detail = (
    alt.Chart(raw_log_data)
    .mark_bar()
    .encode(
        x=alt.X(
            "AGGREGATE_DATE",
            type="temporal",
            timeUnit="yearmonthdate",
            title="Date",
            axis=alt.Axis(
                ticks=True,
                labels=True,
            ),
        ),
        y=alt.Y(
            "EVENT_COUNT",
            type="quantitative",
            title="API Call Count",
            aggregate="sum",
            axis=alt.Axis(tickMinStep=1, labelAlign="center"),
        ),
        color=alt.Color(
            "STATUS",
            scale=alt.Scale(domain=statuses, range=color),
            legend=alt.Legend(
                orient="bottom",
                legendX=130,
                legendY=-40,
                direction="horizontal",
            ),
        ),
        tooltip=[
            alt.Tooltip("API_EVENT_NAME", title="API"),
            alt.Tooltip("STATUS", title="STATUS"),
            alt.Tooltip("EVENT_COUNT", title="API Calls"),
        ],
    )
    .properties(title="API Events 14 Days Detail View")
    .transform_filter(selection)
)


sqlquery = """
SELECT
    replace(api_event_name, '_', ' ') api_event_name,
    max(to_char(api_call_time, 'YYYYMMDD HH24:MI:SS')) AS last_run,
    max(last_status) AS last_status,
    SUM(CASE WHEN status = 'SUCCESS' AND DATE(api_call_time) = CURRENT_DATE() THEN 1 ELSE 0 END) AS success_today,
    SUM(CASE WHEN status = 'WARNING' AND DATE(api_call_time) = CURRENT_DATE() THEN 1 ELSE 0 END) AS warning_today,
    SUM(CASE WHEN status = 'FAILED' AND DATE(api_call_time) = CURRENT_DATE() THEN 1 ELSE 0 END) AS failed_today,
    SUM(CASE WHEN status = 'SUCCESS' AND DATE(api_call_time) = dateadd('day', -1, CURRENT_DATE()) THEN 1 ELSE 0 END) AS success_t_minus1,
    SUM(CASE WHEN status = 'WARNING' AND DATE(api_call_time) = dateadd('day', -1, CURRENT_DATE()) THEN 1 ELSE 0 END) AS warning_t_minus1,
    SUM(CASE WHEN status = 'FAILED' AND DATE(api_call_time) = dateadd('day', -1, CURRENT_DATE()) THEN 1 ELSE 0 END) AS failed_t_minus1,
    SUM(CASE WHEN status = 'SUCCESS' AND DATE(api_call_time) = dateadd('day', -2, CURRENT_DATE()) THEN 1 ELSE 0 END) AS success_t_minus2,
    SUM(CASE WHEN status = 'WARNING' AND DATE(api_call_time) = dateadd('day', -2, CURRENT_DATE()) THEN 1 ELSE 0 END) AS warning_t_minus2,
    SUM(CASE WHEN status = 'FAILED' AND DATE(api_call_time) = dateadd('day', -2, CURRENT_DATE()) THEN 1 ELSE 0 END) AS failed_t_minus2,
    SUM(CASE WHEN status = 'SUCCESS' AND DATE(api_call_time) = dateadd('day', -3, CURRENT_DATE()) THEN 1 ELSE 0 END) AS success_t_minus3,
    SUM(CASE WHEN status = 'WARNING' AND DATE(api_call_time) = dateadd('day', -3, CURRENT_DATE()) THEN 1 ELSE 0 END) AS warning_t_minus3,
    SUM(CASE WHEN status = 'FAILED' AND DATE(api_call_time) = dateadd('day', -3, CURRENT_DATE()) THEN 1 ELSE 0 END) AS failed_t_minus3,
    SUM(CASE WHEN status = 'SUCCESS' AND DATE(api_call_time) = dateadd('day', -4, CURRENT_DATE()) THEN 1 ELSE 0 END) AS success_t_minus4,
    SUM(CASE WHEN status = 'WARNING' AND DATE(api_call_time) = dateadd('day', -4, CURRENT_DATE()) THEN 1 ELSE 0 END) AS warning_t_minus4,
    SUM(CASE WHEN status = 'FAILED' AND DATE(api_call_time) = dateadd('day', -4, CURRENT_DATE()) THEN 1 ELSE 0 END) AS failed_t_minus4,
    SUM(CASE WHEN status = 'SUCCESS' AND DATE(api_call_time) = dateadd('day', -5, CURRENT_DATE()) THEN 1 ELSE 0 END) AS success_t_minus5,
    SUM(CASE WHEN status = 'WARNING' AND DATE(api_call_time) = dateadd('day', -5, CURRENT_DATE()) THEN 1 ELSE 0 END) AS warning_t_minus5,
    SUM(CASE WHEN status = 'FAILED' AND DATE(api_call_time) = dateadd('day', -5, CURRENT_DATE()) THEN 1 ELSE 0 END) AS failed_minus5,
    SUM(CASE WHEN status = 'SUCCESS' AND DATE(api_call_time) = dateadd('day', -6, CURRENT_DATE()) THEN 1 ELSE 0 END) AS success_t_minus6,
    SUM(CASE WHEN status = 'WARNING' AND DATE(api_call_time) = dateadd('day', -6, CURRENT_DATE()) THEN 1 ELSE 0 END) AS warning_t_minus6,
    SUM(CASE WHEN status = 'FAILED' AND DATE(api_call_time) = dateadd('day', -6, CURRENT_DATE()) THEN 1 ELSE 0 END) AS failed_t_minus6
FROM (select a.*, 
             last_value(status) over (
               partition by api_event_name 
               order by api_call_time) as last_status
      from app_schema.api_log a)
WHERE api_call_time >= dateadd('day', -6, CURRENT_DATE()) 
GROUP BY api_event_name
ORDER BY api_event_name
"""
df = session.sql(sqlquery).collect()
df2 = pd.DataFrame(df)

sqlquery = """
SELECT
    to_char(api_call_time, 'YYYYMMDD') AS YYYYMMDD,
    replace(api_event_name, '_', ' ') api_event_name,
    to_char(api_call_time, 'YYYYMMDD HH24:MI:SS') api_call_time,
    status,
    ABS(DATEDIFF('days', current_date(), api_call_time) ) DAYS_AGO,
    call_details, 
    response_status_code, 
    response_text,
FROM app_schema.api_log
WHERE api_call_time >= dateadd('day', -6, CURRENT_DATE()) 
ORDER BY 3 DESC, 2, 1
"""
df3 = session.sql(sqlquery).collect()
df4 = pd.DataFrame(df3)

sqlquery = """
with zero_to_six as (
        select row_number() OVER (ORDER BY NULL) - 1 as DAYS_AGO
        from   TABLE(GENERATOR(rowcount => 7))),
    log as (
        select ABS(DATEDIFF('days', current_date(), api_call_time) ) DAYS_AGO, status
        from app_schema.api_log
        WHERE api_call_time >= dateadd('day', -6, CURRENT_DATE()))
SELECT
    zero_to_six.DAYS_AGO,
    SUM(CASE WHEN status = 'SUCCESS'  THEN 1 ELSE 0 END) AS num_success,
    SUM(CASE WHEN status = 'WARNING'  THEN 1 ELSE 0 END) AS num_warning,
    SUM(CASE WHEN status = 'FAILED'   THEN 1 ELSE 0 END) AS num_failed,
    COUNT(*) num_total
FROM zero_to_six
LEFT JOIN log ON zero_to_six.DAYS_AGO = log.DAYS_AGO
GROUP BY 1
ORDER BY 1"""
df5 = session.sql(sqlquery).collect()
df6 = pd.DataFrame(df3)

new_list = []
counter = 0
for x in df:
    mod3 = counter % 3
    api_name = x[0]
    last_run = x[1]
    last_status = x[2]
    counter += 1
    new_list.append([mod3, api_name, last_run, last_status])

st.subheader("Most Recent Runs For Each API", anchor=False)
c1, c2 = st.columns(2)

with c1:
    for i in new_list:
        mod3 = i[0]
        api_name = i[1]
        last_run = i[2]
        last_status = i[3]
        if mod3 == 0:
            my_str = f"**{api_name.title()}   \n {last_run}   \n {last_status}**"
            if last_status == "SUCCESS":
                st.success(my_str, icon="🟢")
            else:
                st.error(my_str, icon="🔴")
with c2:
    for i in new_list:
        mod3 = i[0]
        api_name = i[1]
        last_run = i[2]
        last_status = i[3]
        if mod3 == 1:
            my_str = f"**{api_name.title()}   \n {last_run}   \n {last_status}**"
            if last_status == "SUCCESS":
                st.success(my_str, icon="🟢")
            else:
                st.error(my_str, icon="🔴")

layer_width = 800
layer_height = 150
layered = (
    log_chart_base.properties(width=layer_width, height=layer_height)
    + log_chart_days.properties(width=layer_width, height=layer_height)
    + log_chart_no_events.properties(width=layer_width, height=layer_height)
)
full_chart = alt.vconcat(
    layered, log_chart_detail.properties(width=layer_width, height=layer_height)
)


op_stats_columns = st.columns((3, 1))

with op_stats_columns[0]:
    st.altair_chart(full_chart, use_container_width=True)

with op_stats_columns[1]:
    st.subheader("Operational Statistics", anchor=False)
    st.caption("Based on last 14 days")
    failure_count = raw_log_data.loc[raw_log_data["STATUS"] == "FAILED"]
    st.metric("Failed Calls", value=failure_count[["EVENT_COUNT"]].sum())
    success_count = raw_log_data.loc[raw_log_data["STATUS"] == "SUCCESS"]
    st.metric("Succeeded Calls", value=success_count[["EVENT_COUNT"]].sum())
    days_no_call = raw_log_data.loc[raw_log_data["EVENT_COUNT"] == 0]
    st.metric(
        "Days without API Events",
        value=days_no_call[["AGGREGATE_DATE"]].count(),
    )




tab_names = []
counter = 0
for x in df5:
    DAYS_AGO = x[0]
    num_success = x[1]
    num_warning = x[2]
    num_failed = x[3]
    num_total = x[4]
    counter += 1
    if num_total >= 0 and num_failed == 0 and num_warning == 0 and num_success > 0:
        if DAYS_AGO == 0:
            t_name = "🟢" + " Today"
        else:
            t_name = "🟢" + f" Today-{DAYS_AGO}"
    else:
        if DAYS_AGO == 0:
            t_name = "🔴" + " Today"
        else:
            t_name = "🔴" + f" Today-{DAYS_AGO}"

    tab_names.append(t_name)

st.divider()


st.header("Historic Summary for Each API")
wh_table = pd.DataFrame(df)

cards_columns = st.columns(wh_table.shape[0])
for key, row in wh_table.iterrows():

    indexes = [i for i in row.index[3:]]
    success_list = [row[i] for i in range(3, 22, 3)]
    warning_list = [row[i] for i in range(4, 23, 3)]
    failure_list = [row[i] for i in range(5, 24, 3)]
    md_table = [
        f"|`{i[0]}`|`{i[1]}`|`{i[2]}`|`{i[3]}`|"
        for i in zip(indexes, success_list, warning_list, failure_list)
    ]
    md_table = "\r".join(md_table)

    markdown_message = f"""
            ##### {row[0].title()}\n
            **Last Run:** {row[1]} \n
            **Last Status:** {row[2]}\n
            |`Day`|`Succeed`|`Warning`|`Failed`|
            |---|---|---|---|
            {md_table}\n
            ####

                """
    with cards_columns[key]:
        if row[2] == "FAILED":
            st.error(markdown_message)
        if row[2] == "SUCCESS":
            st.success(markdown_message)


st.header("Detailed Log Data")

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(tab_names)

with tab1:
    st.subheader("Today's Log Data")
    display_df = df4.loc[df4["DAYS_AGO"].isin([0])]
    

    wh_table = pd.DataFrame(display_df)
    st.dataframe(
        wh_table.style.apply(
            lambda _: wh_table.iloc[::2].applymap(alternating_rows), axis=None
        ),
        use_container_width=True,
    )

   
    csv = convert_df(display_df)

    st.download_button(
        label="Download Today's data as CSV",
        data=csv,
        file_name="download.csv",
        mime="text/csv",
    )

with tab2:
    st.subheader("Yesterday's Log Data")
    display_df = df4.loc[df4["DAYS_AGO"].isin([1])]

    wh_table = pd.DataFrame(display_df)
    st.dataframe(
        wh_table.style.apply(
            lambda _: wh_table.iloc[::2].applymap(alternating_rows), axis=None
        ),
        use_container_width=True,
    )

    csv = convert_df(display_df)
    st.download_button(
        label="Download T-1 data as CSV",
        data=csv,
        file_name="download.csv",
        mime="text/csv",
    )

with tab3:
    st.subheader("Today Minus Two Days Ago Log Data")
    display_df = df4.loc[df4["DAYS_AGO"].isin([2])]

    wh_table = pd.DataFrame(display_df)
    st.dataframe(
        wh_table.style.apply(
            lambda _: wh_table.iloc[::2].applymap(alternating_rows), axis=None
        ),
        use_container_width=True,
    )

    csv = convert_df(display_df)
    st.download_button(
        label="Download T-2 data as CSV",
        data=csv,
        file_name="download.csv",
        mime="text/csv",
    )

with tab4:
    st.subheader("Today Minus Three Days Ago Log Data")
    display_df = df4.loc[df4["DAYS_AGO"].isin([3])]

    wh_table = pd.DataFrame(display_df)
    st.dataframe(
        wh_table.style.apply(
            lambda _: wh_table.iloc[::2].applymap(alternating_rows), axis=None
        ),
        use_container_width=True,
    )

    csv = convert_df(display_df)
    st.download_button(
        label="Download T-3 data as CSV",
        data=csv,
        file_name="download.csv",
        mime="text/csv",
    )

with tab5:
    st.subheader("Today Minus Four Days Ago Log Data")
    display_df = df4.loc[df4["DAYS_AGO"].isin([4])]

    wh_table = pd.DataFrame(display_df)
    st.dataframe(
        wh_table.style.apply(
            lambda _: wh_table.iloc[::2].applymap(alternating_rows), axis=None
        ),
        use_container_width=True,
    )

    csv = convert_df(display_df)
    st.download_button(
        label="Download T-4 data as CSV",
        data=csv,
        file_name="download.csv",
        mime="text/csv",
    )

with tab6:
    st.subheader("Today Minus Five Days Ago Log Data")
    display_df = df4.loc[df4["DAYS_AGO"].isin([5])]

    wh_table = pd.DataFrame(display_df)
    st.dataframe(
        wh_table.style.apply(
            lambda _: wh_table.iloc[::2].applymap(alternating_rows), axis=None
        ),
        use_container_width=True,
    )

    csv = convert_df(display_df)
    st.download_button(
        label="Download T-5 data as CSV",
        data=csv,
        file_name="download.csv",
        mime="text/csv",
    )

with tab7:
    st.subheader("Today Minus Six Days Ago Log Data")
    display_df = df4.loc[df4["DAYS_AGO"].isin([6])]

    wh_table = pd.DataFrame(display_df)
    st.dataframe(
        wh_table.style.apply(
            lambda _: wh_table.iloc[::2].applymap(alternating_rows), axis=None
        ),
        use_container_width=True,
    )

    csv = convert_df(display_df)
    st.download_button(
        label="Download T-6 data as CSV",
        data=csv,
        file_name="download.csv",
        mime="text/csv",
    )






CommonObjects.footer()