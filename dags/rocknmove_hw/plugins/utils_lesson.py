import ast
from io import BytesIO
import csv
import codecs
import requests
import psycopg2
from psycopg2.extras import execute_values
import boto3
from botocore.client import Config

from airflow.hooks.base import BaseHook


def load_from_api(API_URL, data_interval_start, data_interval_end, date_tag):

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': data_interval_start.to_date_string(),
        'end': data_interval_end.to_date_string()
    }

    response = requests.get(url=API_URL, params=payload)
    data = response.json()

    print("=> got data by API - OK")

    connection = BaseHook.get_connection(conn_id='conn_pg')

    with psycopg2.connect(
        dbname=connection.schema,
        user=connection.login,
        host=connection.host,
        port=connection.port,
        password=connection.password,
        sslmode='disable',
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:

        with conn.cursor() as cur:

            print("=> connection to DB - OK")

            rows_to_insert = []
            for el in data:
                row = []

                passback_params = ast.literal_eval(el.get('passback_params', '{}'))

                row.append(el.get('lti_user_id'))
                row.append(passback_params.get('oauth_consumer_key'))
                row.append(passback_params.get('lis_result_sourcedid'))
                row.append(passback_params.get('lis_outcome_service_url'))
                row.append(el.get('is_correct'))
                row.append(el.get('attempt_type'))
                row.append(el.get('created_at'))
                row.append(date_tag)

                rows_to_insert.append(row)

            execute_values(
                cur=cur,
                sql="""
                    INSERT INTO rocknmove_raw_data 
                    (lti_user_id, oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url, is_correct, attempt_type, created_at, date_tag) 
                    VALUES %s
                    ON CONFLICT ON CONSTRAINT unique_check DO NOTHING
                    """,
                argslist=rows_to_insert
            )

            print("=> data insert to DB - OK")


def upload_agg_data_s3(ds, data_interval_start, data_interval_end):

    connection_pg = BaseHook.get_connection(conn_id='conn_pg')
    connection_s3 = BaseHook.get_connection(conn_id='conn_s3')

    start = data_interval_start.to_date_string()
    end = data_interval_end.to_date_string()

    query = """
    SELECT *
    FROM rocknmove_data_agg1
    WHERE period_start = %s
        AND period_end = (%s::date - INTERVAL '1 DAY')::date
    """

    with psycopg2.connect(
        dbname=connection_pg.schema,
        user=connection_pg.login,
        host=connection_pg.host,
        port=connection_pg.port,
        password=connection_pg.password,
        sslmode='disable',
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:

        with conn.cursor() as cur:

            cur.execute(query=query, vars=(start, end))
            data_to_upload = cur.fetchall()

    file = BytesIO()
    writer_wrapper = codecs.getwriter(encoding='utf-8')

    writer = csv.writer(
        writer_wrapper(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )

    writer.writerow(['id', 'period_start', 'period_end', 'attempts_total',
                    'correct_attempts', 'unique_users', 'new_users', 'date_tag'])
    writer.writerows(data_to_upload)
    file.seek(0)

    s3_client = boto3.client(
        's3',
        endpoint_url=connection_s3.host,
        aws_access_key_id=connection_s3.login,
        aws_secret_access_key=connection_s3.password,
        config=Config(signature_version="s3v4")
    )

    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=f"rocknmove/lesson-8/agg_{ds}.csv"
    )


def upload_raw_data_s3(ds, data_interval_start, data_interval_end):

    connection_pg = BaseHook.get_connection(conn_id='conn_pg')
    connection_s3 = BaseHook.get_connection(conn_id='conn_s3')

    start = data_interval_start.to_date_string()
    end = data_interval_end.to_date_string()

    query = """
    SELECT *
    FROM rocknmove_raw_data
    WHERE created_at >= %s
        AND created_at < %s
    """

    with psycopg2.connect(
        dbname=connection_pg.schema,
        user=connection_pg.login,
        host=connection_pg.host,
        port=connection_pg.port,
        password=connection_pg.password,
        sslmode='disable',
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:

        with conn.cursor() as cur:

            cur.execute(query=query, vars=(start, end))
            data_to_upload = cur.fetchall()

    file = BytesIO()
    writer_wrapper = codecs.getwriter(encoding='utf-8')

    writer = csv.writer(
        writer_wrapper(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )

    writer.writerow(['id', 'lti_user_id', 'oauth_consumer_key', 'lis_result_sourcedid',
                    'lis_outcome_service_url', 'is_correct', 'attempt_type', 'created_at', 'date_tag'])
    writer.writerows(data_to_upload)
    file.seek(0)

    s3_client = boto3.client(
        's3',
        endpoint_url=connection_s3.host,
        aws_access_key_id=connection_s3.login,
        aws_secret_access_key=connection_s3.password,
        config=Config(signature_version="s3v4")
    )

    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=f"rocknmove/lesson-8/raw_{ds}.csv"
    )
