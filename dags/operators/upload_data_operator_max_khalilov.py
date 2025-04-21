import psycopg2 as pg # Подключение к базе данных
from io import BytesIO # Библиотека для работы с бинарными данными. В объектном хранилище все хранится в байтах, из-за этого используем данную библиотеку
import csv # Библиотека для работы с CSV файлами
import boto3 as s3 # Библиотека для работы с AWS S3
from botocore.client import Config # Библиотека для работы с AWS S3. Непосредственно для подключения к MinIO 
import codecs # Библиотека для работы с кодировками

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

from typing import Any

class CustomUploadDataOperator(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context: Any):
        sql_query = f"""
            SELECT * FROM maks_khalilov_agr
            WHERE date >= '{context['ds']}'::timestamp
                AND date < '{context['ds']}'::timestamp + INTERVAL '1 days';
        """

        connection = BaseHook.get_connection('conn_pg') # Получаем соединение с базой данных (эти данные находятся в Airflow во вкладке Connections)

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
            cursor = conn.cursor() # Создаем курсор 
            cursor.execute(sql_query) # Выполняем запрос
            data = cursor.fetchall() # Получаем данные

        file = BytesIO() # Создаем объект BytesIO
        
        writer_wrapper = codecs.getwriter('utf-8') # Кодировка

        # Создаем объект writer для записи данных в файл
        writer = csv.writer(
            writer_wrapper(file), # Объект writer для записи данных в файл
            delimiter='\t', # Разделитель
            lineterminator='\n', # Разделитель строк
            quotechar='"', # Кавычки
            quoting=csv.QUOTE_MINIMAL # Режим кавычек
        )

        writer.writerows(data) # Записываем данные в файл
        file.seek(0) # Смещаем курсор в начало файла. Если мы этого не сделаем, то ничего не запишется в файл

        # До этого момента мы создали объект BytesIO, который содержит в себе данные из базы данных. Теперь нам нужно записать эти данные в файл и загрузить его в S3. До этого все хранится в нашей ОС.

        connection = BaseHook.get_connection('conn_s3') # Получаем соединение с S3
        # Создаем клиента для работы с S3
        s3_client = s3.client(
            's3', # Тип клиента. Хоть мы и используем MinIO, но все равно пишем s3 (так как это стандарт и API у них совместимые)
            endpoint_url=connection.host, # Хост    
            aws_access_key_id=connection.login, # Логин
            aws_secret_access_key=connection.password, # Пароль
            config=Config(signature_version='s3v4') # Версия подписи. Для MinIO это обязательное поле
        )

        # Загружаем файл в S3
        s3_client.put_object(
            Body=file, # Тело файла 
            Bucket='default-storage', # Название бакета
            Key=f"admin_{context['ds']}.csv" # Название файла. Делаем так, чтобы нащи файлы не перезатирались и не накладывались друг на друга, и были с уникальными именами.
        )