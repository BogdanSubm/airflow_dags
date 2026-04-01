from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import psycopg2 as pg


from spiridonov_api_sensor import APISensor

DEFAULT_ARGS = {
    'owner': 'spiridonov_a',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 13),
}

def fetch_data(**context):
    import requests

    date_from = context['ds']
    date_to = (datetime.strptime(context['ds'], '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': date_from,
        'end': date_to,
    }
    api_url = 'https://b2b.itresume.ru/api/statistics'

    try:
        response = requests.get(api_url, params=payload, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data:
            context['task_instance'].log.info('No data')
            return

        connection = BaseHook.get_connection('conn_pg')

        with pg.connect(
            dbname='etl',
            sslmode='disable',
            user=connection.login,
            password=connection.password,
            host=connection.host,
            port=connection.port,
            connect_timeout=600,
        ) as conn:
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS spiridonov_api_data (
                        id SERIAL PRIMARY KEY,
                        lti_user_id VARCHAR(255),
                        attempt_type VARCHAR(50),
                        is_correct BOOLEAN,
                        created_at TIMESTAMP,
                        load_date DATE
                )
            ''')

            insert_count = 0
            for record in data:
                cursor.execute('''
                    INSERT INTO spiridonov_api_data 
                    (lti_user_id, attempt_type, is_correct, created_at, load_date)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (
                    record.get('lti_user_id'),
                    record.get('attempt_type'),
                    record.get('is_correct'),
                    record.get('created_at'),
                    context['ds']
                ))
                insert_count += 1
            conn.commit()
    except requests.exceptions.RequestException as e:
        context['task_instance'].log.info(f'Error API: {e}')
        raise
    except Exception as e:
        context['task_instance'].log.error(f'Error DB: {e}')
        raise

def process_api_data(**context):

    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
            dbname='etl',
            sslmode='disable',
            user=connection.login,
            password=connection.password,
            host=connection.host,
            port=connection.port,
    ) as conn:
        cursor = conn.cursor()

        sql_query = f"""
            INSERT INTO spiridonov_agg_table_les9
            SELECT 
                lti_user_id,
                attempt_type,
                COUNT(1) as attempt_count,
                COUNT(CASE WHEN NOT is_correct THEN 1 END) as attempt_failed_count,
                '{context['ds']}'::timestamp as date
            FROM spiridonov_api_data
            WHERE load_date = '{context['ds']}'
            GROUP BY lti_user_id, attempt_type
            ON CONFLICT (date, lti_user_id, attempt_type) 
            DO UPDATE SET 
                attempt_count = EXCLUDED.attempt_count,
                attempt_failed_count = EXCLUDED.attempt_failed_count
        """

        cursor.execute(sql_query)
        conn.commit()
        context['task_instance'].log.info("Data processing completed")

with DAG(
    dag_id='spiridonov_lesson_16_api_sensor_dag',
    tags=['16', 'spiridonov', 'api'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
) as dag:
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    wait_for_api = APISensor(
        task_id = 'wait_for_api',
        date_from = '{{ ds }}',
        date_to = '{{ macros.ds_add(ds, 1) }}',
        mode = 'reschedule',
        poke_interval=300,
        timeout=7200,
    )

    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_api_data,
    )

dag_start >> wait_for_api >> fetch_data >> process_data >> dag_end
