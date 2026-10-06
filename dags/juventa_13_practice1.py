from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pendulum
from juventa.api_to_pg_operator import JApiToPgOperator
from juventa.custom_branch_operator1 import JCustomBranchOperatorNew
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'owner': 'juventa',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': pendulum.datetime(2024, 10, 23, tz="UTC")
}

#API_URL = "https://b2b.itresume.ru/api/statistics"

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
    dag_id='juventa_13_homework1',
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

    dag_end = EmptyOperator(
        task_id='dag_end',
        trigger_rule='always'
    )

    branch = JCustomBranchOperatorNew(
        task_id='branch',
        task_id_exclude = 'load_task',
        weekdays=[0, 4, 6],
    )

    load_task = JApiToPgOperator(
        task_id='load_task',
        date_from = '{{ ds }}',  #'{{ current_month_start(ds) }}',
        date_to = '{{ next_ds }}'#, '{{ current_month_end(ds) }}',
    )

    dag_start >> branch >> load_task

    load_task >> dag_end

