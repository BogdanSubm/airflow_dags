from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
import psycopg2 as pg
import pandas as pd
import json
import io
import calendar

class MonthTemplates:
    @staticmethod
    def start_of_month(date) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        first_day = logical_dt.replace(day=1)
        return first_day.strftime('%Y-%m-%d')
    @staticmethod
    def end_of_month(date) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        last_day_num = calendar.monthrange(logical_dt.year, logical_dt.month)[1]
        last_day = logical_dt.replace(day=last_day_num)
        return last_day.strftime('%Y-%m-%d')
    @staticmethod
    def month_name(date) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        return logical_dt.strftime('%B').lower()
    @staticmethod
    def month_period(date) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        return logical_dt.strftime('%Y-%m')

DEFAULT_ARGS = {
    'owner': 'spiridonov_a',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 1),
}

API_URL = 'https://b2b.itresume.ru/api/statistics'

def fetch_monthly_data(month_start, month_end, **context):
    import requests
    import pendulum

    #execution_date = context['ds']
    #week_start = execution_date
    #week_end = (pendulum.parse(execution_date) + timedelta(days=6)).to_date_string()

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': month_start,
        'end': month_end,
    }

    response = requests.get(API_URL, params=payload)
    data = response.json()

    context['ti'].xcom_push(key='raw_data', value=data)
    #context['ti'].xcom_push(key='week_start', value=week_start)
    #context['ti'].xcom_push(key='week_end', value=week_end)

    return len(data)

def save_month_csv(month_start, month_end, month_period, **context):
    import boto3# as s3
    from botocore.client import Config

    ti=context['ti']
    data = ti.xcom_pull(key='raw_data')

    if not data:
        print(f'Нет данных за период {month_start} - {month_end}')
        return

    df = pd.DataFrame(data)
    df['month_start'] = month_start
    df['month_end'] = month_end

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    #week_start = ti.xcom_pull(key='week_start')
    #week_end = ti.xcom_pull(key='week_end')

    connection = BaseHook.get_connection('conn_s3')
    s3_client = boto3.client(
        's3',
        endpoint_url=connection.host,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version='s3v4'),
    )

    # json_data = json.dumps(data, indent=2, default=str)
    # file_name = f'week_{week_start}_to_{week_end}.json'
    #
    # s3_client.put_object(
    #     Key=file_name,
    #     Bucket='default-storage',
    #     Body=json_data.encode('utf-8'),
    # )
    #
    # df = pd.DataFrame(data)
    # csv_buffer = io.StringIO()
    # df.to_csv(csv_buffer, index=False)

    file_name = f'spiridonov_{month_period}.csv'
    s3_client.put_object(
        Body=csv_buffer.getvalue().encode('utf-8'),
        Key=file_name,
        Bucket='default-storage',
    )

    print(f'Данные за {month_period} загружены в Minio')

    save_to_pg(data, month_start, month_end, month_period)

def save_to_pg(data, month_start, month_end, month_period):
    import ast
    #
    # ti=context['ti']
    # data = ti.xcom_pull(key='raw_data')
    #week_start = ti.xcom_pull(key='week_start')
    #week_end = ti.xcom_pull(key='week_end')

    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname='etl',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port
    ) as conn:
        cursor = conn.cursor()

        cursor.execute('''
            DELETE FROM spiridonov_les10_monthly_raw
            WHERE month_period = %s
        ''', (month_period))

        for record in data:
            passback_params = ast.literal_eval(record.get('passback_params') if record.get('passback_params') else '{}')
            cursor.execute("""
                           INSERT INTO spiridonov_les10_monthly_raw
                           (lti_user_id, is_correct, attempt_type, created_at,
                            oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url,
                            month_start, month_end, month_period, loaded_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                           """, (
                               record.get('lti_user_id'),
                               bool(record.get('is_correct', False)),
                               record.get('attempt_type'),
                               record.get('created_at'),
                               passback_params.get('oauth_consumer_key'),
                               passback_params.get('lis_result_sourcedid'),
                               passback_params.get('lis_outcome_service_url'),
                               month_start, month_end, month_period
                           ))

            conn.commit()

        print('Сырые данные сохранены в PG')

with DAG(
    dag_id='spiridonov_les_10_jinja',
    tags=['spiridonov'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup=False,
    user_defined_macros={
        'month_templates': MonthTemplates
    }
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_monthly_data,
        op_kwargs={
            'month_start': '{{ month_templates.start_of_month(ds) }}',
            'month_end': '{{ month_templates.end_of_month(ds) }}',
            # 'week_start': '{{ ds }}',
            # 'week_end': '{{ macros.ds_add(ds, 6) }}',
        }
    )

    save_csv = PythonOperator(
        task_id='save_month_csv',
        python_callable=save_month_csv,
        op_kwargs={
            'month_start': '{{ month_templates.start_of_month(ds) }}',
            'month_end': '{{ month_templates.end_of_month(ds) }}',
            'month_period': '{{ month_templates.month_period(ds) }}',
        }
    )

start >> fetch_data >> save_csv >> end