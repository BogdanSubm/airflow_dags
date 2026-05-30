from datetime import datetime

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

import psycopg2 as pg


class AggDataPostgresOperator(BaseOperator):
    """
    Параметры:
        start   — начало периода
        end     — конец периода
        conn_id — ID Airflow-подключения (по умолчанию 'conn_pg')
        dbname  — имя БД (по умолчанию 'etl')
    """

    template_fields = ('start', 'end')

    def __init__(self, start, end, conn_id='conn_pg', dbname='etl', **kwargs):
        super().__init__(**kwargs)
        self.start = start
        self.end = end
        self.conn_id = conn_id
        self.dbname = dbname

    def execute(self, context):
        connection = BaseHook.get_connection(self.conn_id)
        with pg.connect(
            dbname=self.dbname,
            sslmode='disable',
            user=connection.login,
            password=connection.password,
            host=connection.host,
            port=connection.port,
            connect_timeout=600,
            keepalives_idle=600,
            tcp_user_timeout=600,
        ) as conn:
            cursor = conn.cursor()

            self.log.info('Удаляю старый агрегат за неделю %s', self.start)
            cursor.execute(
                'DELETE FROM f0rtunaa_agg_table WHERE week_start = %s',
                (self.start,),
            )

            self.log.info('Агрегирую данные за период %s — %s', self.start, self.end)
            cursor.execute(
                """
                INSERT INTO f0rtunaa_agg_table
                    (week_start, lti_user_id, total_attempts, correct_attempts, accuracy)
                SELECT
                    %s                                                             AS week_start,
                    lti_user_id,
                    COUNT(*)                                                       AS total_attempts,
                    SUM(CASE WHEN is_correct THEN 1 ELSE 0 END)                    AS correct_attempts,
                    ROUND(AVG(CASE WHEN is_correct THEN 1.0 ELSE 0 END) * 100, 2) AS accuracy
                FROM f0rtunaa_raw_table
                WHERE created_at >= %s::timestamp
                  AND created_at <  %s::timestamp
                GROUP BY lti_user_id
                """,
                (self.start, self.start, self.end),
            )
            self.log.info('Вставлено строк: %s', cursor.rowcount)

            conn.commit()


DEFAULT_ARGS = {
    'owner': 'f0rtunaa',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 12),
}

API_URL = "https://b2b.itresume.ru/api/statistics"


def load_from_api(start, end, **context):
    import requests
    import ast

    payload = {'client': 'Skillfactory', 'client_key': 'M2MGWS', 'start': start, 'end': end}
    data = requests.get(API_URL, params=payload).json()

    connection = BaseHook.get_connection('conn_pg')
    with pg.connect(
        dbname='etl', sslmode='disable',
        user=connection.login, password=connection.password,
        host=connection.host, port=connection.port,
        connect_timeout=600, keepalives_idle=600, tcp_user_timeout=600,
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'DELETE FROM f0rtunaa_raw_table WHERE created_at >= %s::timestamp AND created_at < %s::timestamp',
            (start, end),
        )
        for el in data:
            pp = ast.literal_eval(el.get('passback_params') or '{}')
            cursor.execute(
                """INSERT INTO f0rtunaa_raw_table
                   (lti_user_id, is_correct, attempt_type, created_at,
                    oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (
                    el.get('lti_user_id'),
                    el.get('is_correct') == 1,
                    el.get('attempt_type'),
                    el.get('created_at'),
                    pp.get('oauth_consumer_key'),
                    pp.get('lis_result_sourcedid'),
                    pp.get('lis_outcome_service_url'),
                ),
            )
        conn.commit()


def upload_data(start, end, **context):
    import csv
    from io import BytesIO
    import codecs
    import boto3 as s3
    from botocore.client import Config

    connection = BaseHook.get_connection('conn_pg')
    with pg.connect(
        dbname='etl', sslmode='disable',
        user=connection.login, password=connection.password,
        host=connection.host, port=connection.port,
        connect_timeout=600, keepalives_idle=600, tcp_user_timeout=600,
    ) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM f0rtunaa_agg_table WHERE week_start = %s', (start,))
        data = cursor.fetchall()

    file = BytesIO()
    writer = csv.writer(
        codecs.getwriter('utf-8')(file),
        delimiter='\t', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_MINIMAL,
    )
    writer.writerows(data)
    file.seek(0)

    conn_s3 = BaseHook.get_connection('conn_s3')
    s3.client(
        's3',
        endpoint_url=conn_s3.host,
        aws_access_key_id=conn_s3.login,
        aws_secret_access_key=conn_s3.password,
        config=Config(signature_version='s3v4'),
    ).put_object(Body=file, Bucket='default-storage', Key=f'f0rtunaa_{start}.csv')


with DAG(
    dag_id='load_from_api_f0rtunaa_3',
    tags=['3', 'f0rtunaa'],
    schedule='@weekly',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end   = EmptyOperator(task_id='dag_end')

    load_from_api_task = PythonOperator(
        task_id='load_from_api',
        python_callable=load_from_api,
        op_kwargs={'start': '{{ ds }}', 'end': '{{ macros.ds_add(ds, 7) }}'},
    )

    agg_data_task = AggDataPostgresOperator(
        task_id='agg_data',
        start='{{ ds }}',
        end='{{ macros.ds_add(ds, 7) }}',
    )

    upload_data_task = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_kwargs={'start': '{{ ds }}', 'end': '{{ macros.ds_add(ds, 7) }}'},
    )

    dag_start >> load_from_api_task >> agg_data_task >> upload_data_task >> dag_end