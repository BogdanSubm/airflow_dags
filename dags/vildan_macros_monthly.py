from datetime import datetime, timedelta
import calendar as cal

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {
    'owner': 'vildan_kharisov',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 2, 1),
}


class MonthTemplates:
    @staticmethod
    def current_month_start(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")

        month_start = logical_dt - timedelta(days=logical_dt.day-1)

        return month_start.strftime("%Y-%m-%d")

    @staticmethod
    def current_month_end(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")

        month_end = logical_dt + timedelta(days=cal.monthrange(logical_dt.year, logical_dt.month)[1]- logical_dt.day)

        return month_end.strftime("%Y-%m-%d")


def upload_data(month_start: str, month_end: str, **context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = f"""
        SELECT * FROM vildan_agg_table_weekly
        WHERE date >= '{month_start}'::timestamp 
              AND date < '{month_end}'::timestamp + INTERVAL '1 days';
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
        tcp_user_timeout=600
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
        Key=f"vildan_{month_start}_{context['ds']}.csv"
    )


def combine_data(month_start: str, month_end: str, **context):
    import psycopg2 as pg

    sql_query = f"""
        INSERT INTO vildan_agg_table_monthly_daily
        SELECT lti_user_id,
               attempt_type,
               COUNT(1),
               COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_failed_count,
               '{month_start}'::timestamp
               ,'{context['ds']}'::timestamp
          FROM vildan_kharisov_table
         WHERE created_at >= '{month_start}'::timestamp 
               AND created_at < '{month_end}'::timestamp + INTERVAL '1 days'
          GROUP BY lti_user_id, attempt_type;
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
        tcp_user_timeout=600
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        conn.commit()


with DAG(
    dag_id="vildan-kharisov-7270_macros_monthly",
    tags=['vildan', '5'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    user_defined_macros={
        "current_month_start": MonthTemplates.current_month_start,
        "current_month_end": MonthTemplates.current_month_end,
    },
    render_template_as_native_obj=True
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    combine_data = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data,
        op_kwargs={
            'month_start': '{{ current_month_start(ds) }}',
            'month_end': '{{ current_month_end(ds) }}',
        }
    )

    upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_kwargs={
            'month_start': '{{ current_month_start(ds) }}',
            'month_end': '{{ current_month_end(ds) }}',
        }
    )

    dag_start >> combine_data >> upload_data >> dag_end
