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
connection = BaseHook.get_connection('conn_pg')
conn_params = {
        'dbname': 'etl',
        'user': connection.login,
        'password': connection.password,
        'host': connection.host,
        'port': connection.port
    }
def extract_data(**context):
    logical_date = context["logical_date"]
    current_date = logical_date.date()

    # Получаем номер текущей недели и год
    current_year, current_week, _ = current_date.isocalendar()

    # Вычисляем дату начала и конца предыдущей календарной недели
    # Начало предыдущей недели (понедельник)
    start_of_previous_week = current_date - timedelta(
        days=current_date.weekday() + 7  # Откатываемся на 7 дней от текущего понедельника
    )
    # Конец предыдущей недели (воскресенье)
    end_of_previous_week = start_of_previous_week + timedelta(days=7)

    start_date = start_of_previous_week.strftime("%Y-%m-%d")
    end_date = end_of_previous_week.strftime("%Y-%m-%d")
    # Получаем подключение через Airflow Hook

    # Параметры подключения

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
            'lis_outcome_service_url': passback_params.get('lis_outcome_service_url'),
            "is_correct" :i.get('is_correct'),
            "attempt_type":i.get('attempt_type'),
            "created_at":i.get('created_at')
        }
        dataS.append(s)

    # Если данных нет — завершаем функцию
    if not dataS:
        print("Нет данных для загрузки")
        return

    # SQL для создания таблицы
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS Practic1_gd (
            lti_user_id VARCHAR,
            oauth_consumer_key VARCHAR,
            lis_result_sourcedid VARCHAR,
            lis_outcome_service_url VARCHAR,
            is_correct VARCHAR,
            attempt_type VARCHAR,
            created_at DATE
        );
    """

    # Создание таблицы и загрузка данных
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Создаём таблицу
                cur.execute(create_table_sql)

                # Подготавливаем данные для вставки
                columns = ['lti_user_id', 'oauth_consumer_key', 'lis_result_sourcedid', 'lis_outcome_service_url','is_correct','attempt_type','created_at']
                data_tuples = [tuple(item[col] for col in columns) for item in dataS]

                # SQL-запрос для вставки
                insert_sql = "INSERT INTO Practic1_gd (lti_user_id, oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url,is_correct,attempt_type,created_at) VALUES (%s, %s, %s, %s, %s, %s, %s)"

                # Вставляем данные партиями
                execute_batch(cur, insert_sql, data_tuples, page_size=1000)

            conn.commit()
            print(f"Успешно загружено {len(dataS)} записей в таблицу Practic1_gd")

    except Exception as e:
        print(f"Ошибка при работе с БД: {e}")
        raise
def agg_Table(**context):
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Шаг 1: Создание таблицы (если не существует)
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS Practic1_gd_agg (
                    attempt_type TEXT,
                    cnt_user BIGINT
                );
                """
                cur.execute(create_table_sql)
                print("Таблица Practic1_gd_agg создана или уже существует")

                # Шаг 2: Агрегация данных — получаем результаты запроса
                agg_query = """
                SELECT
                attempt_type,
                COUNT(lti_user_id) AS cnt_user
                FROM Practic1_gd
                GROUP BY attempt_type;
                """
                cur.execute(agg_query)
                aggregated_data = cur.fetchall()
                print(f"Получено {len(aggregated_data)} записей после агрегации")

                # Шаг 3: Очистка таблицы перед записью новых данных (опционально)
                truncate_sql = "TRUNCATE TABLE Practic1_gd_agg;"
                cur.execute(truncate_sql)

                # Шаг 4: Запись агрегированных данных в таблицу
                insert_sql = """
                INSERT INTO Practic1_gd_agg (attempt_type, cnt_user)
                VALUES (%s, %s);
                """

                if aggregated_data:
                    cur.executemany(insert_sql, aggregated_data)
                    print(f"Успешно записано {len(aggregated_data)} записей в таблицу Practic1_gd_agg")
                else:
                    print("Нет данных для записи в таблицу Practic1_gd_agg")

            conn.commit()
            print("Транзакция успешно завершена")

    except Exception as e:
        print(f"Ошибка при работе с БД: {e}")
        raise


def on_task_failure(context):
    """Callback при ошибке задачи."""
    logging.error(f"Задача {context['task_instance'].task_id} упала: {context['exception']}")

default_args = {
    'owner': 'GD',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Увеличили количество повторов
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_task_failure,  # Callback при ошибке
}

with DAG(
    dag_id='GD_Practic1',
    default_args=default_args,
    description='DAG для извлечения данных из API и агрегации',
    schedule_interval='0 7 * * 1',# каждый понедельник
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
    Agg_Table = PythonOperator(
        task_id='ag_Table',
        python_callable=agg_Table,
        execution_timeout=timedelta(hours=1),  # Таймаут выполнения
    )

    dag_start >> load_from_api >>Agg_Table>> dag_end