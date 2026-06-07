from airflow.hooks.base import BaseHook

API_URL = 'https://b2b.itresume.ru/api/statistics'
BUCKET_NAME = 'default-storage'

RAW_TABLE = 'enjout_raw_data'
AGG_TABLE = 'enjout_agg_data'

CREATE_RAW_TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
        id SERIAL PRIMARY KEY,
        lti_user_id VARCHAR(255),
        is_correct BOOLEAN,
        attempt_type VARCHAR(50),
        created_at TIMESTAMP,
        oauth_consumer_key VARCHAR(255),
        lis_result_sourcedid TEXT,
        lis_outcome_service_url TEXT,
        period_start DATE,
        period_end DATE,
        UNIQUE(lti_user_id, created_at)
    );
"""

CREATE_AGG_TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {AGG_TABLE} (
        id SERIAL PRIMARY KEY,
        attempt_type VARCHAR(50),
        period_start DATE,
        period_end DATE,
        total_attempts INTEGER,
        correct_attempts INTEGER,
        incorrect_attempts INTEGER,
        unique_users INTEGER,
        avg_correct_rate DECIMAL(10, 4),
        min_created_at TIMESTAMP,
        max_created_at TIMESTAMP,
        UNIQUE(attempt_type, period_start, period_end)
    );
"""


def _get_db_connection():
    import psycopg2 as pg

    connection = BaseHook.get_connection('conn_pg')
    return pg.connect(
        dbname='etl',
        sslmode='disable',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600,
    )


def _parse_passback_params(passback_str):
    import ast

    if not passback_str:
        return {}
    try:
        return ast.literal_eval(passback_str)
    except (ValueError, SyntaxError):
        return {}


def load_raw_data(week_start: str, week_end: str, execution_date: str):
    import requests

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': week_start,
        'end': week_end,
    }

    print(f'Запрос API за период {week_start} — {week_end} (ds={execution_date})')
    response = requests.get(API_URL, params=payload, timeout=60)
    response.raise_for_status()
    data = response.json()
    print(f'Получено {len(data)} записей')

    with _get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(CREATE_RAW_TABLE_SQL)

        cursor.execute(
            f'DELETE FROM {RAW_TABLE} WHERE period_start = %s AND period_end = %s',
            (week_start, week_end),
        )

        insert_sql = f"""
            INSERT INTO {RAW_TABLE}
            (lti_user_id, is_correct, attempt_type, created_at,
             oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url,
             period_start, period_end)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (lti_user_id, created_at) DO NOTHING
        """

        inserted = 0
        for el in data:
            passback_params = _parse_passback_params(el.get('passback_params'))
            cursor.execute(
                insert_sql,
                (
                    el.get('lti_user_id'),
                    True if el.get('is_correct') == 1 else False,
                    el.get('attempt_type'),
                    el.get('created_at'),
                    passback_params.get('oauth_consumer_key'),
                    passback_params.get('lis_result_sourcedid'),
                    passback_params.get('lis_outcome_service_url'),
                    week_start,
                    week_end,
                ),
            )
            inserted += 1

        conn.commit()
        print(f'Загружено {inserted} записей в {RAW_TABLE}')


def aggregate_data(week_start: str, week_end: str, execution_date: str):
    aggregation_sql = f"""
        INSERT INTO {AGG_TABLE}
        (attempt_type, period_start, period_end,
         total_attempts, correct_attempts, incorrect_attempts,
         unique_users, avg_correct_rate, min_created_at, max_created_at)
        SELECT
            attempt_type,
            %s::date AS period_start,
            %s::date AS period_end,
            COUNT(*) AS total_attempts,
            COUNT(CASE WHEN is_correct THEN 1 END) AS correct_attempts,
            COUNT(CASE WHEN NOT is_correct THEN 1 END) AS incorrect_attempts,
            COUNT(DISTINCT lti_user_id) AS unique_users,
            ROUND(AVG(CASE WHEN is_correct THEN 1.0 ELSE 0.0 END), 4) AS avg_correct_rate,
            MIN(created_at) AS min_created_at,
            MAX(created_at) AS max_created_at
        FROM {RAW_TABLE}
        WHERE period_start = %s::date AND period_end = %s::date
        GROUP BY attempt_type
        ON CONFLICT (attempt_type, period_start, period_end) DO UPDATE SET
            total_attempts = EXCLUDED.total_attempts,
            correct_attempts = EXCLUDED.correct_attempts,
            incorrect_attempts = EXCLUDED.incorrect_attempts,
            unique_users = EXCLUDED.unique_users,
            avg_correct_rate = EXCLUDED.avg_correct_rate,
            min_created_at = EXCLUDED.min_created_at,
            max_created_at = EXCLUDED.max_created_at
    """

    with _get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(CREATE_AGG_TABLE_SQL)
        cursor.execute(aggregation_sql, (week_start, week_end, week_start, week_end))
        conn.commit()
        print(f'Агрегация завершена для периода {week_start} — {week_end} (ds={execution_date})')


def export_aggregated_to_s3(week_start: str, week_end: str, execution_date: str):
    import csv
    import codecs
    from io import BytesIO

    import boto3
    from botocore.client import Config

    sql_query = f"""
        SELECT attempt_type, period_start, period_end,
               total_attempts, correct_attempts, incorrect_attempts,
               unique_users, avg_correct_rate, min_created_at, max_created_at
        FROM {AGG_TABLE}
        WHERE period_start = %s::date AND period_end = %s::date
        ORDER BY attempt_type
    """

    with _get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query, (week_start, week_end))
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

    file = BytesIO()
    writer = csv.writer(
        codecs.getwriter('utf-8')(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
    )
    writer.writerow(columns)
    writer.writerows(rows)
    file.seek(0)

    connection = BaseHook.get_connection('conn_s3')
    s3_client = boto3.client(
        's3',
        endpoint_url=connection.host,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version='s3v4'),
    )

    key = f'enjout_agg_{week_start}_to_{week_end}_{execution_date}.csv'
    s3_client.put_object(Body=file, Bucket=BUCKET_NAME, Key=key)
    print(f'CSV загружен в {BUCKET_NAME}/{key}')
