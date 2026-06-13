# -*- coding: utf-8 -*-
# Выгружаем таблицу в CSV и отправляем в объектное хранилище

import psycopg2 as pg
import boto3 as s3
from io import BytesIO
import csv
import codecs
from botocore.client import Config
from airflow.hooks.base import BaseHook

def export_to_s3(table_name: str, **context):
    """
    Выгружаем данные за текущий день в S3.
    Структура папок: table_name/load_date=2026-01-01/файл.csv
    """
    log = context['ti'].log
    ds = context['ds']
    log.info(f'☁️ Экспортирую {table_name} за {ds} в S3')
    
    # 1. Забираем данные из БД
    pg_conn = BaseHook.get_connection('conn_pg')
    with pg.connect(
        dbname='etl',
        user=pg_conn.login,
        password=pg_conn.password,
        host=pg_conn.host,
        port=pg_conn.port,
        sslmode='disable',
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE load_date = %s", (ds,))
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]  # заголовки для CSV
    
    if not data:
        raise ValueError(f"❌ Нет данных для экспорта из {table_name} за {ds}")
    
    # 2. Собираем CSV в оперативной памяти (чтобы не плодить файлы)
    file = BytesIO()
    writer = csv.writer(
        codecs.getwriter('utf-8')(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
    )
    writer.writerow(columns)
    writer.writerows(data)
    file.seek(0)
    
    # 3. Отправляем в S3
    s3_conn = BaseHook.get_connection('conn_s3')
    s3_client = s3.client(
        's3',
        endpoint_url=s3_conn.host,
        aws_access_key_id=s3_conn.login,
        aws_secret_access_key=s3_conn.password,
        config=Config(signature_version='s3v4'),
    )
    
    # Ключ с партиционированием — удобно читать в Spark/Pandas
    key = f"{table_name}/load_date={ds}/{table_name}_{ds}.csv"
    
    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=key,
    )
    
    log.info(f'✅ Файл загружен: {key}')
