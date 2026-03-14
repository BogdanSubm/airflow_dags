from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import logging
import psycopg2
import requests
import ast
from airflow.hooks.base import BaseHook
from psycopg2.extras import execute_batch

def extract_data(**context):
    logical_date = context["logical_date"]
    yesterday = logical_date.date() - timedelta(days=1)
    start_date = yesterday.strftime("%Y-%m-%d")
    end_date = logical_date.strftime("%Y-%m-%d")
    # Получаем подключение через Airflow Hook
    connection = BaseHook.get_connection('conn_pg')

    # Параметры подключения
    conn_params = {
        'dbname': 'etl',
        'user': connection.login,
        'password': connection.password,
        'host': connection.host,
        'port': connection.port
    }

    # Запрос данных из API
    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': start_date,
        'end': end_date
    }
    r = requests.get('https://b2b.itresume.ru/api/statistics', params=payload)
    print(f"URL запроса: {r.url}")

    if r.status_code != 200:
        raise Exception(f"Ошибка API: {r.status_code}, ответ: {r.text}")

    res = r.json()  # Предполагаем, что API возвращает JSON

    # Обработка данных
    dataS = []
    for i in res:
        passback_params_str = i.get('passback_params')
        if passback_params_str:
            try:
                passback_params = ast.literal_eval(passback_params_str)
            except (SyntaxError, ValueError):
                print(f"Не удалось распарсить passback_params: {passback_params_str}")
                passback_params = {}
        else:
            passback_params = {}

        s = {
            'lti_user_id': i.get('lti_user_id'),
            'oauth_consumer_key': passback_params.get('oauth_consumer_key'),
            'lis_result_sourcedid': passback_params.get('lis_result_sourcedid'),
            'lis_outcome_service_url': passback_params.get('lis_outcome_service_url')
        }
        dataS.append(s)

    # Если данных нет — завершаем функцию
    if not dataS:
        print("Нет данных для загрузки")
        return

    # SQL для создания таблицы
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS Test_gd (
            lti_user_id VARCHAR(255),
            oauth_consumer_key VARCHAR(100),
            lis_result_sourcedid VARCHAR(100),
            lis_outcome_service_url VARCHAR(100)
        );
    """

    # Создание таблицы и загрузка данных
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Создаём таблицу
                cur.execute(create_table_sql)

                # Подготавливаем данные для вставки
                columns = ['lti_user_id', 'oauth_consumer_key', 'lis_result_sourcedid', 'lis_outcome_service_url']
                data_tuples = [tuple(item[col] for col in columns) for item in dataS]

                # SQL-запрос для вставки
                insert_sql = "INSERT INTO Test_gd (lti_user_id, oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url) VALUES (%s, %s, %s, %s)"

                # Вставляем данные партиями
                execute_batch(cur, insert_sql, data_tuples, page_size=1000)

            conn.commit()
            print(f"Успешно загружено {len(dataS)} записей в таблицу Test_gd")

    except Exception as e:
        print(f"Ошибка при работе с БД: {e}")
        raise
def on_task_failure(context):
    """Callback при ошибке задачи."""
    logging.error(f"Задача {context['task_instance'].task_id} упала: {context['exception']}")

default_args = {
    'owner': 'GD',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Увеличили количество повторов
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_task_failure,  # Callback при ошибке
}

with DAG(
    dag_id='GD_Extract_Api',
    default_args=default_args,
    description='DAG для извлечения данных из API',
    schedule_interval='3 8 * * *',
    catchup=True,
    tags=['gd', 'api', 'extract'],
    max_active_runs=1,  # Ограничение одновременных запусков
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    load_from_api = PythonOperator(
        task_id='load_from_api',
        python_callable=extract_data,
        execution_timeout=timedelta(hours=1),  # Таймаут выполнения
    )

    dag_start >> load_from_api >> dag_end