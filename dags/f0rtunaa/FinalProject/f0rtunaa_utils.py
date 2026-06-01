from OP.f0rtunaa_pgcon import _get_pg_conn
from airflow.hooks.base import BaseHook

def upload_to_s3(table_name, **context):
    import csv
    from io import BytesIO
    import codecs
    import boto3
    from botocore.client import Config

    # Читаем данные из Postgres
    with _get_pg_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f'SELECT * FROM {table_name}',
        )
        data = cursor.fetchall()

    # Формируем CSV
    file = BytesIO()
    writer = csv.writer(
        codecs.getwriter('utf-8')(file),
        delimiter='\t', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_MINIMAL,
    )
    writer.writerows(data)
    file.seek(0)

    # Загружаем в S3
    conn_s3 = BaseHook.get_connection('conn_s3')
    boto3.client(
        's3',
        endpoint_url=conn_s3.host,
        aws_access_key_id=conn_s3.login,
        aws_secret_access_key=conn_s3.password,
        config=Config(signature_version='s3v4'),
    ).put_object(
        Body=file,
        Bucket='default-storage',
        Key=f'{table_name}_{context["ds"]}.csv',
    )
