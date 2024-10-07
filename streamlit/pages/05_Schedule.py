import streamlit as st
from datetime import datetime as dt
from snowflake.snowpark.context import get_active_session
from commons import CommonObjects
from snowflake.snowpark import functions as F
import time

st.set_page_config(
    page_title="Capi-API DEMO",
    page_icon="Logo.png",
    layout="wide",
    initial_sidebar_state="expanded",
)

session = get_active_session()

CommonObjects.header()
st.header("Schedule Tweets !!")
st.divider()
controllers_metadata = {
    "days": {"min_value": 1, "max_value": 7, "step": 1, "label": "**Every (n) Days**"},
    "hours": {"min_value": 1, "step": 1, "label": "**Every (n) Hours**"},
    "minutes": {"min_value": 5, "step": 5, "label": "**Every (n) Minutes**"},
}
events = ["SCHEDULE MY TWEETS....TweetJobName "]
schedules_table = "app_schema.SCHEDULES"


def modify_schedule(task_name: str, action: str):
    try:
        session.call("app_orchestration.update_task", task_name, action)
    except Exception as e:
        st.error(f"Error modifying task \n\n {e}")


class ScheduleGroup:
    controllers_metadata = {
        "days": {
            "min_value": 1,
            "max_value": 7,
            "step": 1,
            "label": "**Every (n) Days**",
        },
        "hours": {"min_value": 1, "step": 1, "label": "**Every (n) Hours**"},
        "minutes": {"min_value": 5, "step": 5, "label": "**Every (n) Minutes**"},
    }
    events = ["SCHEDULE_MY_TWEET"]
    schedules_table = "app_schema.SCHEDULES"

    @classmethod
    def create_group(cls, key: str) -> str:
        scheduling_columns = st.columns((1, 3))
        schedule_period = scheduling_columns[1].radio(
            "",
            options=controllers_metadata.keys(),
            format_func=lambda x: str(x).title(),
            horizontal=True,
            key=f"sched_{key}",
        )
        sched_kwargs = controllers_metadata.get(schedule_period)
        schedule_time = scheduling_columns[0].number_input(
            **sched_kwargs, key=f"time_{key}"
        )
        if schedule_period == "minutes":
            return f"{schedule_time} {schedule_period.upper()}"

        if schedule_period == "hours":
            return f"USING CRON 0 */{schedule_time} * * * America/Los_Angeles"

        if schedule_period == "days":
            return f"USING CRON 1 * */{schedule_time} * * America/Los_Angeles"


class ScheduleCard:
    def __init__(self, name, created, type, schedule, status) -> None:
        self.name = name
        self.created = created
        self.type = type
        self.schedule = schedule
        self.status = status

    def render_card(self):
        with st.expander(
            f"**{self.name}** $~~$ {'🔴' if self.status == 'SUSPENDED' else '🟢'}"
        ):
            card_columns = st.columns(2)
            card_columns[0].caption(self.type)
            card_columns[0].markdown(
                f"""
                        **Created On** : {self.created}\n
                        **Schedule**: {self.schedule}
                        """
            )
            if self.status == "SUSPENDED":
                card_columns[1].button(
                    "Resume",
                    key=f"resume_{self.name}",
                    use_container_width=True,
                    on_click=modify_schedule,
                    args=[self.name, "RESUME"],
                )
            else:
                card_columns[1].button(
                    "Suspend",
                    key=f"suspend_{self.name}",
                    use_container_width=True,
                    on_click=modify_schedule,
                    args=[self.name, "SUSPEND"],
                )
            if card_columns[1].button(
                "Delete", key=f"delete_{self.name}", use_container_width=True
            ):
                card_columns[1].button(
                    "Confirm",
                    key=f"confirm_{self.name}",
                    use_container_width=True,
                    on_click=modify_schedule,
                    args=[self.name, "DROP"],
                )
            # new_schedule = ScheduleGroup.create_group(key=self.name)
            # card_columns[1].button(
            #     "Disable Schedule",
            #     key=f"disable_{self.name}",
            #     use_container_width=True,
            # )


try:
    schedules = session.table(schedules_table).to_pandas()
    if schedules.shape[0] > 0:
        schedules_panel_cols = st.columns(2)
        for k, v in schedules.iterrows():
            with schedules_panel_cols[1 if k % 2 else 0]:
                ScheduleCard(
                    name=v["SCHEDULE_NAME"],
                    created=v["CREATED_ON"],
                    type=v["EVENT_TYPE"],
                    schedule=v["SCHEDULE"],
                    status=v["STATUS"],
                ).render_card()


except:
    st.info("No Schedules Table Found, please refer to installation guide.")

st.divider()
event_type = st.selectbox("**Event**", options=events)
add_controllers = st.columns(3)

schedule_period = add_controllers[1].radio(
    "",
    options=controllers_metadata.keys(),
    format_func=lambda x: str(x).title(),
    horizontal=True,
)
sched_kwargs = controllers_metadata.get(schedule_period)
schedule_time = add_controllers[0].number_input(**sched_kwargs)

today = (
    str(dt.now().isoformat(" ", "seconds"))
    .replace("-", "")
    .replace(" ", "_")
    .replace(":", "")
)
task_name = f"""{event_type.replace(" ","").upper()}_{today}"""
stored_proc = (
    "app_schema.POST_TWEET_BATCH"
    )
wh = "xs_wh"


if schedule_period == "minutes":
    schedule = f"{schedule_time} {schedule_period.upper()}"

if schedule_period == "hours":
    schedule = f"USING CRON 0 */{schedule_time} * * * America/Los_Angeles"

if schedule_period == "days":
    schedule = f"USING CRON 1 * */{schedule_time} * * America/Los_Angeles"




if st.button("Create Schedule"):
    try:
        session.call(
            "app_orchestration.create_task", wh, schedule, task_name, stored_proc
        )
        log_dict = {
            "SCHEDULE_NAME": task_name,
            "CREATED_ON": today,
            "EVENT_TYPE": event_type,
            "SCHEDULE": schedule_period,
            
        }
        record_save = (
            session.range(1)
            .with_columns(
                ["SCHEDULE_NAME", "CREATED_ON", "EVENT_TYPE", "SCHEDULE", "STATUS"],
                [
                    F.lit(task_name),
                    F.lit(dt.now().date()),
                    F.lit(event_type),
                    F.lit(schedule),
                    F.lit("ACTIVE"),
                ],
            )
            .select(["SCHEDULE_NAME", "CREATED_ON", "EVENT_TYPE", "SCHEDULE", "STATUS"])
        )
        record_save.write.mode("append").save_as_table(
            schedules_table,
        )

        st.success("Schedule Created!")
        time.sleep(4)
        st.experimental_rerun()
    except Exception as e:
        st.error(e)







