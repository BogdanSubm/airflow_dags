from airflow.hooks.base import BaseHook

def upload_data (**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv 
    import boto3 as s3 
    from botocore.client import Config
    import codecs 

    sql_query = f"""
            SELECT * 
            FROM tkachev_agg_table
            WHERE date >= '{context['ds']}'::timestamp
              AND date < '{context['ds']}'::timestamp + INTERVAL '1 days';
    """

    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname="etl",
        sslmode="disable",
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

    writer_wrapper = codecs.getwriter("utf-8")

    writer = csv.writer(
        writer_wrapper(file),
        delimiter="\t",
        lineterminator="\n",
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )

    writer.writerows(data)
    file.seek(0)

    connection = BaseHook.get_connection('conn_s3')

    s3_client = s3.client(
        "s3",
        endpoint_url=connection.host,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version="s3v4")
    )

    s3_client.put_object(
        Body=file,
        Bucket="default-storage",
        Key=f"tkachev_{context['ds']}.csv"
    )
    