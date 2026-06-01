from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from postgres_operator import PostgresOperator

DEFAULT_ARGS = {
    'owner': 'nikifforushkina',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2026, 6, 1),
}

config = [
    {
        'table_name': 'nikifforushkina_agg_by_user',
        'table_ddl': """
            CREATE TABLE IF NOT EXISTS nikifforushkina_agg_by_user (
                lti_user_id            TEXT,
                attempt_type           TEXT,
                attempt_count          BIGINT,
                attempt_failed_count   BIGINT,
                week_start             TIMESTAMP
            );
        """,
        'table_dml': """
            DELETE FROM nikifforushkina_agg_by_user
             WHERE week_start = '{{ current_week_start(ds) }}'::timestamp;

            INSERT INTO nikifforushkina_agg_by_user
            SELECT lti_user_id,
                   attempt_type,
                   COUNT(1)                                                    AS attempt_count,
                   COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END)           AS attempt_failed_count,
                   '{{ current_week_start(ds) }}'::timestamp                  AS week_start
              FROM nikifforushkina_raw_table
             WHERE created_at >= '{{ current_week_start(ds) }}'::timestamp
                   AND created_at < '{{ current_week_end(ds) }}'::timestamp + INTERVAL '1 days'
             GROUP BY lti_user_id, attempt_type;
        """,
        'need_to_export': True,
    },
    {
        'table_name': 'nikifforushkina_agg_by_type',
        'table_ddl': """
            CREATE TABLE IF NOT EXISTS nikifforushkina_agg_by_type (
                attempt_type           TEXT,
                attempt_count          BIGINT,
                attempt_failed_count   BIGINT,
                week_start             TIMESTAMP
            );
        """,
        'table_dml': """
            DELETE FROM nikifforushkina_agg_by_type
             WHERE week_start = '{{ current_week_start(ds) }}'::timestamp;

            INSERT INTO nikifforushkina_agg_by_type
            SELECT attempt_type,
                   COUNT(1)                                                    AS attempt_count,
                   COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END)           AS attempt_failed_count,
                   '{{ current_week_start(ds) }}'::timestamp                  AS week_start
              FROM nikifforushkina_raw_table
             WHERE created_at >= '{{ current_week_start(ds) }}'::timestamp
                   AND created_at < '{{ current_week_end(ds) }}'::timestamp + INTERVAL '1 days'
             GROUP BY attempt_type;
        """,
        'need_to_export': False,
    },
]


class WeekTemplates:
    @staticmethod
    def current_week_start(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")
        current_week_start = logical_dt - timedelta(days=logical_dt.weekday())
        return current_week_start.strftime("%Y-%m-%d")

    @staticmethod
    def current_week_end(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")
        current_week_end = logical_dt + timedelta(days=6 - logical_dt.weekday())
        return current_week_end.strftime("%Y-%m-%d")


def export_to_s3(table_name: str, week_start: str, ds: str):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    pg_conn = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname='etl',
        sslmode='disable',
        user=pg_conn.login,
        password=pg_conn.password,
        host=pg_conn.host,
        port=pg_conn.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600,
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT * FROM {table_name}
             WHERE week_start = '{week_start}'::timestamp;
        """)
        data = cursor.fetchall()

    file = BytesIO()
    writer = csv.writer(
        codecs.getwriter('utf-8')(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
    )
    writer.writerows(data)
    file.seek(0)

    s3_conn = BaseHook.get_connection('conn_s3')
    s3_client = s3.client(
        's3',
        endpoint_url=s3_conn.host,
        aws_access_key_id=s3_conn.login,
        aws_secret_access_key=s3_conn.password,
        config=Config(signature_version="s3v4"),
    )
    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=f"nikifforushkina_{table_name}_{week_start}_{ds}.csv",
    )


with DAG(
    dag_id="dynamic_agg_dag",
    tags=['nikifforushkina', 'final'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=4,
    user_defined_macros={
        "current_week_start": WeekTemplates.current_week_start,
        "current_week_end":   WeekTemplates.current_week_end,
    },
    render_template_as_native_obj=True,
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end   = EmptyOperator(task_id='dag_end')

    for table_config in config:
        table_name = table_config['table_name']

        #Создание таблицы если не существует
        create_table = PostgresOperator(
            task_id=f'create_table__{table_name}',
            sql=table_config['table_ddl'],
        )

        #Наполнение таблицы идемпотентно
        fill_table = PostgresOperator(
            task_id=f'fill_table__{table_name}',
            sql=table_config['table_dml'],
        )

        dag_start >> create_table >> fill_table

        #Экспорт в S3
        if table_config['need_to_export']:
            export_task = PythonOperator(
                task_id=f'export_to_s3__{table_name}',
                python_callable=export_to_s3,
                op_kwargs={
                    'table_name': table_name,
                    'week_start': '{{ current_week_start(ds) }}',
                    'ds':         '{{ ds }}',
                },
            )
            fill_table >> export_task >> dag_end
        else:
            fill_table >> dag_end