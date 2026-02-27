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

DEFAULT_ARGS = {
    'owner': 'spiridonov_a',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 11),
}

API_URL = 'https://b2b.itresume.ru/api/statistics'

def fetch_data_from_api(**context):
    import requests
    import pendulum

    execution_date = context['ds']
    week_start = execution_date
    week_end = (pendulum.parse(execution_date) + timedelta(days=6)).to_date_string()

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': week_start,
        'end': week_end,
    }

    response = requests.get(API_URL, params=payload)
    data = response.json()

    context['ti'].xcom_push(key='raw_data', value=data)
    context['ti'].xcom_push(key='week_start', value=week_start)
    context['ti'].xcom_push(key='week_end', value=week_end)

    return len(data)

def save_raw_to_minio(**context):
    ti=context['ti']
    data = ti.xcom_pull(key='raw_data')
    week_start = ti.xcom_pull(key='week_start')
    week_end = ti.xcom_pull(key='week_end')

    minio_hook = S3Hook(
        aws_conn_id='conn_s3',
        endpoint_url='http://95.163.241.236:9001'
    )

    json_data = json.dumps(data, indent=2, default=str)

    file_name = f'week_{week_start}_to_{week_end}.json'

    minio_hook.load_string(
        string_data=json_data,
        key=file_name,
        bucket_name='default-storage',
        replace=True,
    )

    df = pd.DataFrame(data)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    csv_file_name = f'week_{week_start}_to_{week_end}.csv'
    minio_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=csv_file_name,
        bucket_name='default-storage',
        replace=True,
    )

    print('Данные загружены в Minio')

def save_raw_to_pg(**context):
    import ast
    ti=context['ti']
    data = ti.xcom_pull(key='raw_data')
    week_start = ti.xcom_pull(key='week_start')
    week_end = ti.xcom_pull(key='week_end')

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
            DELETE FROM spiridonov_table_8_9_extra_raw
            WHERE week_start = %s AND week_end = %s
        ''', (week_start, week_end))

        for record in data:
            passback_params = ast.literal_eval(record.get('passback_params') if record.get('passback_params') else '{}')
            cursor.execute("""
                           INSERT INTO spiridonov_table_8_9_extra_raw
                           (lti_user_id, is_correct, attempt_type, created_at,
                            oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url,
                            week_start, week_end, loaded_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                           """, (
                               record.get('lti_user_id'),
                               bool(record.get('is_correct', False)),
                               record.get('attempt_type'),
                               record.get('created_at'),
                               passback_params.get('oauth_consumer_key'),
                               passback_params.get('lis_result_sourcedid'),
                               passback_params.get('lis_outcome_service_url'),
                               week_start, week_end
                           ))

            conn.commit()

            print('Сырые данные сохранены в PG')

def agg_week_data(**context):
    week_start = context['ti'].xcom_pull(key='week_start')
    week_end = context['ti'].xcom_pull(key='week_end')

    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname='etl',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            DELETE FROM spiridonov_agg_table_8_9_extra_stats
            WHERE week_start = %s and week_end = %s
        """, (week_start, week_end))

        cursor.execute(f"""
            INSERT INTO spiridonov_agg_table_8_9_extra_stats
            (week_start, week_end, total_attempts, correct_attempts, success_rate,
            unique_users, attempts_per_user_avg, min_created_at, max_created_at)
            SELECT
                %s, %s,
                COUNT(*) AS total_attempts,
                SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) as correct_attempts,
                AVG(CASE WHEN is_correct THEN 1 ELSE 0 END) * 100 as success_rate,
                COUNT(DISTINCT lti_user_id) AS unique_users,
                COUNT(*)::float / COUNT(DISTINCT lti_user_id) as attempts_per_user_avg,
                MIN(created_at) as min_created_at,
                max(created_at) as max_created_at
            FROM spiridonov_table_8_9_extra_raw
            WHERE week_start = %s and week_end = %s
        """, (week_start, week_end, week_start, week_end))

        conn.commit()

        cursor.execute(f"""
            SELECT * FROM spiridonov_agg_table_8_9_extra_stats
            WHERE week_start = %s and week_end = %s
        """, (week_start, week_end))

        agg_data = cursor.fetchone()
        conn.commit()

        save_agg_to_minio(agg_data, week_start, week_end, context)

def save_agg_to_minio(agg_data, week_start, week_end, context):
    minio_hook = S3Hook(
        aws_conn_id='conn_s3',
        endpoint_url='http://95.163.241.236:9001'
    )

    agg_dict = {
        'week_start': week_start,
        'week_end': week_end,
        'total_attempts': agg_data[2],
        'correct_attempts': agg_data[3],
        'success_rate': float(agg_data[4]),
        'unique_users': agg_data[5],
        'attempts_per_user_avg': float(agg_data[6]) if agg_data[6] else 0,
        'min_created_at': str(agg_data[7]),
        'max_created_at': str(agg_data[8]),
        'calculated_at': datetime.now().isoformat()
    }

    json_data = json.dumps(agg_dict, indent=2)
    file_name = f'agg_week_{week_start}_to_{week_end}.json'

    minio_hook.load_string(
        string_data=json_data,
        key=file_name,
        bucket_name='default-storage',
        replace=True
    )

    df = pd.DataFrame([agg_dict])
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    csv_file_name = f'agg_week_{week_start}_to_{week_end}.csv'
    minio_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=csv_file_name,
        bucket_name='default-storage',
        replace=True
    )

    print('Аггрегация сохранена в minio')

with DAG(
    dag_id='spiridonov_extra_8_9',
    tags=['spiridonov'],
    schedule='0 0 * * 1',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_api
    )

    save_to_minio = PythonOperator(
        task_id='save_to_minio',
        python_callable=save_raw_to_minio
    )

    save_to_pg = PythonOperator(
        task_id='save_to_pg',
        python_callable=save_raw_to_pg
    )

    aggregate = PythonOperator(
        task_id='aggregate_data',
        python_callable=agg_week_data
    )

    start >> fetch_data
    
    fetch_data >> [save_to_minio, save_to_pg]

    save_to_pg >> aggregate

    [save_to_minio, aggregate] >> end