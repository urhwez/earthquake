import logging
from datetime import datetime, timedelta
from airflow.timetables.interval import CronDataIntervalTimetable
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

DAG_ID = "api_to_s3"

LAYER = "raw"
SOURCE = "earthquake"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

args = {
    "start_date": pendulum.datetime(2026, 2, 2, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def get_dates(**context) -> tuple[str, str]:
    
    logging.info(context["data_interval_end"])
    logging.info(context["data_interval_start"])
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
  
    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):

    start_date, end_date = get_dates(**context)

    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}"
    logging.info(url)
    logging.info(f"Start load for dates: {start_date}/{end_date}")
    
    con = duckdb.connect()
    con.sql(f"""
        SET TIMEZONE='UTC';
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;
    """)

    logging.info(f"Start load for {start_date}")

    con.sql(
        f"""
            COPY (
                SELECT *
                FROM read_csv_auto(
                    'https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}'
                )
            )
            TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.parquet';
        """
        )
    con.close()
    logging.info(f"✅ Download for date success: {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule = CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    default_args=args,
    tags=["s3", "raw"],
    catchup=True,
    max_active_tasks=1,
    max_active_runs=1
) as dag:

    start = EmptyOperator(task_id="start")

    load = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(task_id="end")

    start >> load >> end