from airflow.hooks.base import BaseHook

from enjout_exam.config import S3_BUCKET, S3_PREFIX


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


def build_idempotency_sql(table_name: str) -> str:
    return (
        f"DELETE FROM {table_name} "
        f"WHERE period_start = '{{{{ previous_week_start(ds) }}}}'::date "
        f"AND period_end = '{{{{ previous_week_end(ds) }}}}'::date"
    )


def build_insert_sql(table_name: str, table_dml: str) -> str:
    return f'INSERT INTO {table_name}\n{table_dml.strip()}'


def check_data_quality(
    table_name: str,
    week_start: str,
    week_end: str,
    min_rows: int,
    execution_date: str,
):
    with _get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f'''
            SELECT COUNT(*) FROM {table_name}
            WHERE period_start = %s::date AND period_end = %s::date
            ''',
            (week_start, week_end),
        )
        row_count = cursor.fetchone()[0]

    print(
        f'Проверка {table_name}: {row_count} строк за {week_start}—{week_end} '
        f'(ds={execution_date}, min_rows={min_rows})'
    )

    if row_count < min_rows:
        raise ValueError(
            f'Таблица {table_name}: найдено {row_count} строк, '
            f'ожидалось не менее {min_rows}'
        )


def export_table_to_s3(
    table_name: str,
    week_start: str,
    week_end: str,
    execution_date: str,
):
    import csv
    import codecs
    from io import BytesIO

    import boto3
    from botocore.client import Config

    sql_query = f'''
        SELECT * FROM {table_name}
        WHERE period_start = %s::date AND period_end = %s::date
        ORDER BY 1
    '''

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

    key = f'{S3_PREFIX}/{table_name}_{week_start}_to_{week_end}_{execution_date}.csv'
    s3_client.put_object(Body=file, Bucket=S3_BUCKET, Key=key)
    print(f'Экспорт завершён: {S3_BUCKET}/{key} ({len(rows)} строк)')
