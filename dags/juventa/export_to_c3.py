from airflow.hooks.base import BaseHook
import psycopg2 as pg
# байтовый объект
from io import BytesIO
import csv
# библиотека для общения с объектными хранилищами
import boto3 as s3
# для подключения к minio
from botocore.client import Config
import codecs
import logging

log = logging.getLogger("airflow.task")

def save_table_to_c3(table_name:str, date_start:str, date_end:str, **context):
    #Выгружает данные из PostgreSQL в MinIO (S3) в формате TSV/CSV
    log.info(f"Начало выгрузки таблицы '{table_name}' за период {date_start} - {date_end}")

    sql_query = f"""
            SELECT * from {table_name}
            WHERE date_start >= '{date_start}'::timestamp
              and date_start < '{date_end}'::timestamp + interval '1 days'             
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

        if not data:
            log.warning(f"Нет данных в таблице '{table_name}' за период {date_start} - {date_end}")
            return


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
        Key=f"juventa/raw/j_{table_name}_{date_start}-{date_end}.csv"
    )

    log.info(f"Файл успешно загружен: juventa/raw/j_{table_name}_{date_start}-{date_end}.csv")