from airflow.hooks.base import BaseHook
from airflow.operators.python import get_current_context

def upload_to_s3(table_name):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    # получаем контекст
    context = get_current_context()
    log = context['ti'].log
    ds = context['ds'] # дата выполнения даг
    log.info(f'Start export {table_name}, date: {ds}')
    # выборка всех данных за дату
    sql_query = f"""
        SELECT *
        FROM {table_name}
        WHERE load_date = %s
    """

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
        cursor.execute(sql_query, (ds,))
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    log.info(f'Rows fetched: {len(data)}')

    # исключение если нет данных
    if not data:
        raise ValueError(f'No data found in {table_name}')

    file = BytesIO()

    writer_wrapper = codecs.getwriter('utf-8')

    writer = csv.writer(
        writer_wrapper(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
    )

    writer.writerow(columns)
    writer.writerows(data)
    file.seek(0)

    connection = BaseHook.get_connection('conn_s3')

    s3_client = s3.client(
        's3',
        endpoint_url=connection.host,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version='s3v4'),
    )

    key = f'{table_name}/load_date={ds}/{table_name}_{ds}.csv'

    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=key,
    )

    log.info(f'Uploaded to S3: {key}')