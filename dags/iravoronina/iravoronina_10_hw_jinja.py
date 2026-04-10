from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pendulum

DEFAULT_ARGS = {
    'owner': 'iravoronina',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2026, 3, 7),
}

API_URL = "https://b2b.itresume.ru/api/statistics"

class MonthTemplates:
    @staticmethod
    def current_month_start(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")
        month_start = logical_dt.replace(day=1)
        return month_start.strftime("%Y-%m-%d")

    @staticmethod
    def current_month_end(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")
        next_month = (logical_dt.replace(day=1) + timedelta(days=32)).replace(day=1)
        month_end = next_month - timedelta(days=1)
        return month_end.strftime("%Y-%m-%d")

def load_from_api(month_start: str, month_end: str, **context):
    import requests
    import psycopg2 as pg
    import ast

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': month_start,
        'end': month_end
    }
    response = requests.get(API_URL, params=payload)
    data = response.json()

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

         #ИДЕМПОТЕНТНОСТЬ: Удаляем данные за этот период перед вставкой
        cursor.execute("""
        DELETE FROM iravoronina_raw_table 
        WHERE created_at >= %s
          AND created_at <= %s
    """, (month_start, month_end))

        for el in data:
            row = []
            passback_params = ast.literal_eval(el.get('passback_params') if el.get('passback_params') else '{}')
            row.append(el.get('lti_user_id'))
            row.append(True if el.get('is_correct') == 1 else False)
            row.append(el.get('attempt_type'))
            row.append(el.get('created_at'))
            row.append(passback_params.get('oauth_consumer_key'))
            row.append(passback_params.get('lis_result_sourcedid'))
            row.append(passback_params.get('lis_outcome_service_url'))

            cursor.execute("INSERT INTO iravoronina_raw_table VALUES (%s, %s, %s, %s, %s, %s, %s)", row)

        conn.commit()


def upload_raw_data(month_start: str, month_end: str, **context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs
    
    sql_query = """
        SELECT * FROM iravoronina_raw_table
        WHERE created_at >= %s::date
          AND created_at <= %s::date;
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
        cursor.execute(sql_query, (month_start, month_end))
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
        Key=f"iravoronina_raw_month_{month_start}.csv"
    )


with DAG(
    dag_id="iravoronina_10_hw_jinja",
    tags=['iravoronina'],
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

    load_from_api = PythonOperator(
        task_id='load_from_api',
        python_callable=load_from_api,
        op_kwargs={
            'month_start': '{{ current_month_start(ds) }}',
            'month_end': '{{ current_month_end(ds) }}',
        }
    )

    upload_raw_data = PythonOperator(
        task_id='upload_raw_data',
        python_callable=upload_raw_data,
        op_kwargs={
            'month_start': '{{ current_month_start(ds) }}',
            'month_end': '{{ current_month_end(ds) }}',
        }
    )

    dag_start >> load_from_api >> upload_raw_data >> dag_end

    