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
class MonthTime:
    @staticmethod
    def StartMonth(Dateds):
        """
        Возвращает дату начала месяца для переданной даты.

        Args:
            Dateds (str): Дата в формате 'YYYY-MM-DD'
        Returns:
            str: Дата начала месяца в формате 'YYYY-MM-DD'
        """
        date_obj = datetime.strptime(Dateds, "%Y-%m-%d")
        start_of_month = date_obj.replace(day=1)
        return start_of_month.strftime("%Y-%m-%d")

    @staticmethod
    def EndMonth(Dateds):
        """
        Возвращает дату конца месяца + 1 день для переданной даты.
        Args:
            Dateds (str): Дата в формате 'YYYY-MM-DD'
        Returns:
            str: Дата конца месяца + 1 день в формате 'YYYY-MM-DD'
        """
        date_obj = datetime.strptime(Dateds, "%Y-%m-%d")

        # Определяем первый день следующего месяца
        if date_obj.month == 12:
            # Если декабрь, переходим на январь следующего года
            first_day_next_month = datetime(date_obj.year + 1, 1, 1)
        else:
            # Для остальных месяцев просто увеличиваем номер месяца на 1
            first_day_next_month = datetime(date_obj.year, date_obj.month + 1, 1)

        # Конец текущего месяца — это день перед первым днём следующего месяца
        end_of_current_month = first_day_next_month - timedelta(days=1)

        # Добавляем 1 день к концу месяца
        next_day = end_of_current_month + timedelta(days=1)

        return next_day.strftime("%Y-%m-%d")





def extract_data(start_date,end_date,**context):
    #logical_date = context["logical_date"]
    #current_date = logical_date.date()

    # Получаем номер текущей недели и год
    #current_year, current_week, _ = current_date.isocalendar()

    # Вычисляем дату начала и конца предыдущей календарной недели
    # Начало предыдущей недели (понедельник)
    #start_of_previous_week = current_date - timedelta(
    #days=current_date.weekday() + 7  # Откатываемся на 7 дней от текущего понедельника
    #)
    # Конец предыдущей недели (воскресенье)
    #end_of_previous_week = start_of_previous_week + timedelta(days=7)

    #start_date = start_of_previous_week.strftime("%Y-%m-%d")
    #end_date = end_of_previous_week.strftime("%Y-%m-%d")
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
            "created_at":i.get('created_at'),
            "Logic_date":f'{start_date}-{end_date}'
        }
        dataS.append(s)

    # Если данных нет — завершаем функцию
    if not dataS:
        print("Нет данных для загрузки")
        return

    # SQL для создания таблицы
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS Practic2_gd (
            lti_user_id VARCHAR,
            oauth_consumer_key VARCHAR,
            lis_result_sourcedid VARCHAR,
            lis_outcome_service_url VARCHAR,
            is_correct VARCHAR,
            attempt_type VARCHAR,
            created_at DATE,
            Logic_date VARCHAR
        );
    """

    # Создание таблицы и загрузка данных
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Создаём таблицу (если ещё не существует)
                cur.execute(create_table_sql)

                # Формируем значение Logic_date
                logic_date_value = f'{start_date}-{end_date}'

                # Удаляем существующие записи с этим Logic_date
                delete_sql = "DELETE FROM Practic2_gd WHERE Logic_date = %s"
                cur.execute(delete_sql, (logic_date_value,))
                deleted_count = cur.rowcount
                print(f"Удалено {deleted_count} записей с Logic_date = '{logic_date_value}'")

                # Подготавливаем данные для вставки
                columns = ['lti_user_id', 'oauth_consumer_key', 'lis_result_sourcedid',
                            'lis_outcome_service_url', 'is_correct', 'attempt_type',
                            'created_at', 'Logic_date']
                data_tuples = [
                    tuple(item[col] for col in columns)
                    for item in dataS
                ]

                # SQL‑запрос для вставки
                insert_sql = """
                    INSERT INTO Practic2_gd
                    (lti_user_id, oauth_consumer_key, lis_result_sourcedid,
                    lis_outcome_service_url, is_correct, attempt_type,
                    created_at, Logic_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """

                # Вставляем данные партиями
                execute_batch(cur, insert_sql, data_tuples, page_size=1000)
                inserted_count = len(data_tuples)

            conn.commit()
            print(f"Успешно загружено {inserted_count} записей в таблицу Practic2_gd")
            if deleted_count > 0:
                print(f"Перед загрузкой было удалено {deleted_count} старых записей")

    except Exception as e:
        print(f"Ошибка при работе с БД: {e}")
        raise
def agg_Table(**context):
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Шаг 1: Создание таблицы (если не существует)
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS Practic2_gd_agg (
                    attempt_type TEXT,
                    cnt_user BIGINT
                );
                """
                cur.execute(create_table_sql)
                print("Таблица Practic2_gd_agg создана или уже существует")

                # Шаг 2: Агрегация данных — получаем результаты запроса
                agg_query = """
                SELECT
                attempt_type,
                COUNT(lti_user_id) AS cnt_user
                FROM Practic2_gd
                GROUP BY attempt_type;
                """
                cur.execute(agg_query)
                aggregated_data = cur.fetchall()
                print(f"Получено {len(aggregated_data)} записей после агрегации")

                # Шаг 3: Очистка таблицы перед записью новых данных (опционально)
                truncate_sql = "TRUNCATE TABLE Practic2_gd_agg;"
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
def upload_data(**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = f"""
        SELECT * FROM Practic2_gd_agg;
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
        Bucket='gdi',
        Key=f"gd_{context['ds']}.csv"
    )


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
    dag_id='GD_Practic2',
    default_args=default_args,
    description='DAG для извлечения данных из API и агрегации c jinja',
    schedule_interval='@daily',
    catchup=True,
    tags=['gd', 'api', 'extract'],
    max_active_runs=1,
    user_defined_macros={
        'currentMStart':MonthTime.StartMonth,
        'currentMEnd':MonthTime.EndMonth,}  
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    load_from_api = PythonOperator(
        task_id='load_from_api',
        python_callable=extract_data,
        op_kwargs={
        'start_date':'{{currentMStart(ds)}}',
        'end_date':'{{currentMEnd(ds)}}',},
        execution_timeout=timedelta(hours=1),  # Таймаут выполнения
    )
    load_to_s3 = PythonOperator(
        task_id='load_to_s3_minio',
        python_callable=upload_data,
        execution_timeout=timedelta(hours=1),  # Таймаут выполнения
    )
    Agg_Table = PythonOperator(
        task_id='ag_Table',
        python_callable=agg_Table,
        execution_timeout=timedelta(hours=1),  # Таймаут выполнения
    )

    dag_start >> load_from_api >>Agg_Table>>load_to_s3 >>dag_end