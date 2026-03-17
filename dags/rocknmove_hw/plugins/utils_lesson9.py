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


def check_tables():

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

            cur.execute(
                query="""
                CREATE TABLE IF NOT EXISTS rocknmove_raw_data (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                    , lti_user_id TEXT NOT NULL
                    , oauth_consumer_key TEXT
                    , lis_result_sourcedid TEXT
                    , lis_outcome_service_url TEXT
                    , is_correct INT CHECK (is_correct in (0, 1) OR is_correct is NULL)
                    , attempt_type TEXT CHECK (attempt_type in ('run', 'submit'))
                    , created_at TIMESTAMP
                    , date_tag TEXT
                    , CONSTRAINT unique_check UNIQUE (lti_user_id, created_at)
                )
                """
            )

            cur.execute(
                query="""
                CREATE TABLE IF NOT EXISTS rocknmove_users (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                    , lti_user_id TEXT NOT NULL
                    , first_activity TIMESTAMP NOT NULL
                    , CONSTRAINT unique_check_2 UNIQUE (lti_user_id)
                )
                """
            )

            cur.execute(
                query="""
                CREATE TABLE IF NOT EXISTS rocknmove_data_agg1 (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                    , period_start DATE
                    , period_end DATE
                    , attempts_total INTEGER
                    , correct_attempts INTEGER
                    , unique_users INTEGER
                    , new_users INTEGER
                    , date_tag TEXT
                    , CONSTRAINT unique_check_3 UNIQUE (period_start, period_end)
                )    
                """
            )

    print("=> tables make/exist - OK")


def delete_named_tables():

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

            cur.execute(
                query="""
                DROP TABLE IF EXISTS rocknmove_raw_data, rocknmove_users, rocknmove_data_agg1
                """
            )
    print("=> tables delete - OK")


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


def add_users(data_interval_start, data_interval_end):

    connection = BaseHook.get_connection('conn_pg')

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

            cur.execute(
                query="""
                SELECT DISTINCT 
                    lti_user_id
                    , min(created_at) AS first_activity
                FROM rocknmove_raw_data
                WHERE created_at >= %s 
                    AND created_at < %s
                GROUP BY lti_user_id
                """,
                vars=(data_interval_start.to_date_string(),
                      data_interval_end.to_date_string())
            )
            users_to_insert = cur.fetchall()

            execute_values(
                cur=cur,
                sql="""
                    INSERT INTO rocknmove_users 
                        (lti_user_id, first_activity) 
                    VALUES %s
                    ON CONFLICT ON CONSTRAINT unique_check_2 DO NOTHING
                    """,
                argslist=users_to_insert
            )


def aggregate_data_1(data_interval_start, data_interval_end):

    connection = BaseHook.get_connection('conn_pg')

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

            start = data_interval_start.to_date_string()
            end = data_interval_end.to_date_string()

            cur.execute(
                query="""
                SELECT
                    %s AS period_start
                    , (%s::date - INTERVAL '1 DAY')::date AS period_end
                    , count(1) AS attempts_total
                    , count(CASE WHEN d.is_correct=1 THEN 1 ELSE NULL END) AS correct_attempts
                    , count(DISTINCT d.lti_user_id) AS unique_users
                    , count(DISTINCT CASE WHEN u.lti_user_id IS NULL THEN d.lti_user_id ELSE NULL END) AS new_users
                    , date_tag
                FROM rocknmove_raw_data d
                LEFT JOIN (SELECT DISTINCT lti_user_id FROM rocknmove_raw_data WHERE created_at < %s) u ON d.lti_user_id=u.lti_user_id
                WHERE created_at >= %s
                    AND created_at < %s
                GROUP BY date_tag
                    """,

                vars=(start, end, start, start, end)
            )

            agg_to_insert = cur.fetchall()

            execute_values(
                cur=cur,
                sql="""
                INSERT INTO rocknmove_data_agg1
                    (period_start, period_end, attempts_total, correct_attempts, unique_users, new_users, date_tag) 
                VALUES %s
                ON CONFLICT ON CONSTRAINT unique_check_3 DO NOTHING
                """,
                argslist=agg_to_insert
            )


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
