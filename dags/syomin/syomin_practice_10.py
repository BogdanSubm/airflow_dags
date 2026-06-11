from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {
    'owner': 'syomin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 11),
}

API_URL = "https://b2b.itresume.ru/api/statistics"

def load_from_api(week_start, week_end, **kwargs):
    import requests
    import psycopg2 as pg
    import ast

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': week_start,
        'end': week_end,
    }

    response = requests.get(API_URL, params=payload)
    data = response.json()
    save_row_data(data, week_start, week_end)

def save_row_data(data, week_start, week_end):
    import psycopg2 as pg
    import ast

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
            DELETE FROM syomin_10_raw_table
            WHERE week_start = %s and week_end = %s
        """, (week_start, week_end))

        for record in data:
            passback_params = ast.literal_eval(record.get('passback_params') if record.get('passback_params') else '{}')
            cursor.execute(f"""
                INSERT INTO syomin_10_raw_table
                (lti_user_id, is_correct, attempt_type, created_at, 
                 oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url,
                 week_start, week_end, loaded_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                record.get('lti_user_id'),
                True if record.get('is_correct') == 1 else False,
                record.get('attempt_type'),
                record.get('created_at'),
                passback_params.get('oauth_consumer_key'),
                passback_params.get('lis_result_sourcedid'),
                passback_params.get('lis_outcome_service_url'),
                week_start, week_end
            ))
        conn.commit()

def agg_day_data(week_start, week_end, **kwargs):
    import psycopg2 as pg

    connection = BaseHook.get_connection('conn_pg')
    with pg.connect(
        dbname='etl',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
    ) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM syomin_10_raw_table
            WHERE week_start = %s AND week_end = %s
        """, (week_start, week_end))
        count = cursor.fetchone()[0]

        if count == 0:
            print(f"Нет данных за период {week_start} - {week_end}, пропускаем агрегацию")
            return

        cursor.execute(f"""
            DELETE FROM syomin_10_agg_table
            WHERE week_start = %s AND week_end = %s
        """, (week_start, week_end))

        cursor.execute(f"""
            INSERT INTO syomin_10_agg_table
            (week_start, week_end, total_attempts, correct_attempts, success_rate,
            unique_users, attempts_per_user_avg, min_created_at, max_created_at)
            SELECT
                %s, %s,
                COUNT(*) AS total_attempts,
                SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) as correct_attempts,
                AVG(CASE WHEN is_correct THEN 1 ELSE 0 END) * 100 as success_rate,
                COUNT(DISTINCT lti_user_id) AS unique_users,
                COUNT(*)::float / NULLIF(COUNT(DISTINCT lti_user_id), 0) as attempts_per_user_avg,
                MIN(created_at) as min_created_at,
                MAX(created_at) as max_created_at
            FROM syomin_10_raw_table
            WHERE week_start = %s AND week_end = %s
        """, (week_start, week_end, week_start, week_end))
        conn.commit()

with DAG(
    dag_id='syomin_practice_89',
    tags=['10', 'syomin'],
    schedule='0 0 * * 1',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    # Передаём даты через Jinja-шаблоны
    load_from_api = PythonOperator(
        task_id='load_from_api',
        python_callable=load_from_api,
        op_kwargs={
            'week_start': '{{ ds }}',
            'week_end': '{{ macros.ds_add(ds, 6) }}',   # добавляем 6 дней к ds
        },
    )

    agg_day_data = PythonOperator(
        task_id='agg_day_data',
        python_callable=agg_day_data,
        op_kwargs={
            'week_start': '{{ ds }}',
            'week_end': '{{ macros.ds_add(ds, 6) }}',
        },
    )

    dag_start >> load_from_api >> agg_day_data >> dag_end