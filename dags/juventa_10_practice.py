from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pendulum


DEFAULT_ARGS = {
    'owner': 'juventa',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': pendulum.datetime(2024, 10, 23, tz="UTC")
}

API_URL = "https://b2b.itresume.ru/api/statistics"

#создаю свои Templates!
class MonthTemplates:
    @staticmethod
    #возвращает начало месяца (строка)
    def current_month_start(date) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        current_month_start = logical_dt.replace(day=1)
        return current_month_start.strftime('%Y-%m-%d')

    @staticmethod
    #возвращает конец месяц (строка)
    def current_month_end(date) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        if logical_dt.month == 12:
            next_month = logical_dt.replace(year=logical_dt.year + 1, month=1, day=1)
        else:
            next_month = logical_dt.replace(month=logical_dt.month + 1, day=1)
        return (next_month - timedelta(days=1)).strftime('%Y-%m-%d')

#загружаем данные из api и сохраняем в сырой слой
def load_from_api(month_start:str, month_end:str, **context):
    import requests
    import pendulum
    import psycopg2 as pg
    import ast

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': month_start,
        'end': month_end
    }
    response = requests.get(API_URL, params=payload, timeout=60)
    response.raise_for_status()
    data = response.json()

    print(f"Запрос с {month_start} по {month_end}, получено: {len(data)} записей")

    #данные которые ранее получили из API
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
        delete_query = '''
            DELETE FROM juventa_raw_month_table
            where created_at >= %s::timestamp
              and created_at < %s::timestamp + interval '1 day'        
            '''
        cursor.execute(delete_query,(month_start,month_end))
        print(f"Удалены данные в БД PostgreSQL за период с {month_start} по {month_end}")

        for el in data:
            row = []
            passback_params = ast.literal_eval(el.get('passback_params') or '{}')
            row.append(el.get('lti_user_id'))
            row.append(True if el.get('is_correct') == 1 else False)
            row.append(el.get('attempt_type'))
            row.append(el.get('created_at'))
            row.append(passback_params.get('oauth_consumer_key'))
            row.append(passback_params.get('lis_result_sourcedid'))
            row.append(passback_params.get('lis_outcome_service_url'))

            cursor.execute("INSERT INTO juventa_raw_month_table VALUES(%s, %s, %s, %s, %s, %s, %s)", row)
        conn.commit()
        print('Сырые данные сохранены в БД PostgreSQL')

#Сохраняем в объектное хранилище
def save_raw_to_minio(month_start:str, month_end:str, **context):
    import psycopg2 as pg
    # байтовый объект
    from io import BytesIO
    import csv
    # библиотека для общения с объектными хранилищами
    import boto3 as s3
    # для подключения к minio
    from botocore.client import Config
    import codecs

    sql_query = f"""
            SELECT * from juventa_raw_month_table
            WHERE created_at >= '{month_start}'::timestamp
              and created_at < '{month_end}'::timestamp + interval '1 days'             
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
        cursor.execute(sql_query)  # выполняем запрос
        data = cursor.fetchall()  # возвращает структуру данных - массив в массивах

    file = BytesIO()  # файловый объект
    writer_wrapper = codecs.getwriter('utf-8')

    writer = csv.writer(
        writer_wrapper(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
    )

    writer.writerows(data)  # записываем данные из селекта в файл
    file.seek(0)  # возвращаем курсор ффайла в начало

    connection = BaseHook.get_connection('conn_s3')  # для подключения к объектному хранидищу

    s3_client = s3.client(
        's3',
        endpoint_url=connection.host,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version='s3v4'),
    )

    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        #Key=f"juventa_{start}_{end}.csv"
        Key=f"juventa/raw/j_m_{month_start}-{month_end}.csv"
    )

    print(f"Файл juventa/raw/j_m_{month_start}_{month_end}.csv загружен")


with DAG(
    dag_id='juventa_10_practice',
    #schedule='0 0 * * 1',  # Понедельник в 00:00 UTC
    schedule='@daily',
    #catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    #Регистрирую свои templates
    user_defined_macros={
        "current_month_start": MonthTemplates.current_month_start,
        "current_month_end": MonthTemplates.current_month_end
    },
    render_template_as_native_obj=True

) as dag:
    dag_start = EmptyOperator(task_id='dag_start')

    dag_end = EmptyOperator(task_id='dag_end')

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_from_api,
        op_kwargs={
            'month_start': '{{ current_month_start(ds) }}',
            'month_end': '{{ current_month_end(ds) }}',
        }
    )

    raw_export_task = PythonOperator(
        task_id='raw_export_task',
        python_callable=save_raw_to_minio,
        op_kwargs={
            'month_start': '{{ current_month_start(ds) }}',
            'month_end': '{{ current_month_end(ds) }}',
        }
    )
    dag_start >> load_task

    load_task >> raw_export_task >> dag_end

