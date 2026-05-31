from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from postgres_operator import PostgresOperator
from api_to_pg_operator import APIToPgOperator

DEFAULT_ARGS = {
    'owner': 'nikifforushkina',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2026, 5, 29),
}


class WeekTemplates:
    @staticmethod
    def current_week_start(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")
        current_week_start = logical_dt - timedelta(days=logical_dt.weekday())
        return current_week_start.strftime("%Y-%m-%d")

    @staticmethod
    def current_week_end(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")
        current_week_end = logical_dt + timedelta(days=6 - logical_dt.weekday())
        return current_week_end.strftime("%Y-%m-%d")


class MonthTemplates:
    @staticmethod
    def current_month_start(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")
        current_month_start = logical_dt.replace(day=1)
        return current_month_start.strftime("%Y-%m-%d")

    @staticmethod
    def current_month_end(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")
        if logical_dt.month == 12:
            next_month_first = logical_dt.replace(year=logical_dt.year + 1, month=1, day=1)
        else:
            next_month_first = logical_dt.replace(month=logical_dt.month + 1, day=1)
        current_month_end = next_month_first - timedelta(days=1)
        return current_month_end.strftime("%Y-%m-%d")


def upload_data(week_start: str, week_end: str, ds: str):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = f"""
        SELECT * FROM nikifforushkina_agg_table_weekly
        WHERE date >= '{week_start}'::timestamp
              AND date < '{week_end}'::timestamp + INTERVAL '1 days';
    """

    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname='etl',
        sslmode='disable',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600,
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        data = cursor.fetchall()

    file = BytesIO()
    writer_wrapper = codecs.getwriter('utf-8')
    writer = csv.writer(
        writer_wrapper(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )
    writer.writerows(data)
    file.seek(0)

    connection = BaseHook.get_connection('conn_s3')

    s3_client = s3.client(
        's3',
        endpoint_url=connection.host,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version="s3v4"),
    )

    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=f"nikifforushkina_{week_start}_{ds}.csv"
    )


with DAG(
    dag_id="combine_api_data_weekly",
    tags=['nikifforushkina', '5'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    user_defined_macros={
        "current_week_start":  WeekTemplates.current_week_start,
        "current_week_end":    WeekTemplates.current_week_end,
        "current_month_start": MonthTemplates.current_month_start,
        "current_month_end":   MonthTemplates.current_month_end,
    },
    render_template_as_native_obj=True
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end   = EmptyOperator(task_id='dag_end')

    combine_data = PostgresOperator(
        task_id='combine_data',
        sql="""
            INSERT INTO nikifforushkina_agg_table_weekly
            SELECT lti_user_id,
                   attempt_type,
                   COUNT(1),
                   COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_failed_count,
                   '{{ current_week_start(ds) }}'::timestamp
              FROM nikifforushkina_raw_table
             WHERE created_at >= '{{ current_week_start(ds) }}'::timestamp
                   AND created_at < '{{ current_week_end(ds) }}'::timestamp + INTERVAL '1 days'
             GROUP BY lti_user_id, attempt_type;
        """,
    )

    upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_kwargs={
            'week_start': '{{ current_week_start(ds) }}',
            'week_end':   '{{ current_week_end(ds) }}',
            'ds':         '{{ ds }}',
        }
    )

    fetch_monthly = APIToPgOperator(
        task_id='fetch_and_save_monthly',
        month_start='{{ current_month_start(ds) }}',
        month_end='{{ current_month_end(ds) }}',
    )

    dag_start >> combine_data >> upload_data >> dag_end
    dag_start >> fetch_monthly >> dag_end