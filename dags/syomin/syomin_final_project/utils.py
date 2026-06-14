def export_table_to_s3(table_name, bucket_name, s3_conn_id, **context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs
    from airflow.hooks.base import BaseHook

    ds = context['ds']  #дата выполнения DAG (YYYY-MM-DD)

    #получаю данные из Postgre
    pg_conn = BaseHook.get_connection('conn_pg')
    with pg.connect(
        dbname='etl',
        user=pg_conn.login,
        password=pg_conn.password,
        host=pg_conn.host,
        port=pg_conn.port,
        sslmode='disable',
        connect_timeout=600,
    ) as conn:
        cursor = conn.cursor()
        #выбираю только строки за текущий день (агрегаты посчитаны)
        sql = f"""
            SELECT * FROM {table_name}
            WHERE purchase_date = %s
        """
        cursor.execute(sql, (ds,))
        data = cursor.fetchall()
        #получаю имена колонок для заголовка CSV
        colnames = [desc[0] for desc in cursor.description]
        
    if not data:
        print(f"Нет данных за {ds} для {table_name}, экспорт пропущен")
        return

    #формирую CSV в памяти
    file = BytesIO()
    writer_wrapper = codecs.getwriter('utf-8')
    writer = csv.writer(
        writer_wrapper(file),
        delimiter=',',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )
    writer.writerow(colnames) #заголовки
    writer.writerows(data)
    file.seek(0)

    #загрузка CSV в S3
    s3_conn = BaseHook.get_connection(s3_conn_id)
    s3_client = s3.client(
        's3',
        endpoint_url=s3_conn.host,
        aws_access_key_id=s3_conn.login,
        aws_secret_access_key=s3_conn.password,
        config=Config(signature_version='s3v4'),
    )
    key = f"exports/{table_name}/ds={ds}/{table_name}_{ds}.csv"
    s3_client.put_object(
        Body=file,
        Bucket=bucket_name,
        Key=key
    )
    print(f"Таблица {table_name} выгружена в s3://{bucket_name}/{key}")