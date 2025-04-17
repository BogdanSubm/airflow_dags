from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

# Параметры по умолчанию
DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 4, 12)
}

class WeekTemplates:
    @staticmethod
    def current_week_start(date) -> str:  # Убран data_type
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        current_week_start = logical_dt - timedelta(days=logical_dt.weekday())
        return current_week_start.strftime('%Y-%m-%d')

    @staticmethod
    def current_week_end(date) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        current_week_end = logical_dt + timedelta(days=6 - logical_dt.weekday())
        return current_week_end.strftime('%Y-%m-%d')

def upload_data(week_start: str, week_end: str, execution_date: str):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    # Используем week_start и week_end для фильтрации данных за неделю
    sql_query = f"""
        SELECT * FROM admin_agg_table
        WHERE date >= '{week_start}'::timestamp
            AND date < '{week_end}'::timestamp + INTERVAL '1 days';
    """

    connection = BaseHook.get_connection('conn_pg')
    with pg.connect(
        dbname='etl',
        sslmode='disable',
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
    writer_wrapper = codecs.getwriter('utf-8')
    writer = csv.writer(
        writer_wrapper(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )
    writer.writerows(data)
    file.seek(0)

    connection = BaseHook.get_connection('conn_s3')
    s3_client = s3.client(
        's3',
        endpoint_url=connection.host,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version='s3v4')
    )
    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=f"admin_{execution_date}.csv"  # Используем execution_date для имени файла
    )

def combine_data(week_start: str, week_end: str, execution_date: str):
    import psycopg2 as pg

    # Используем week_start и week_end для агрегации данных за неделю
    sql_query = f"""
        INSERT INTO maks_khalilov_agr
        SELECT lti_user_id,
               attempt_type,
               COUNT(1),
               COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_fails_count,
               '{execution_date}'::timestamp
          FROM admin_table
         WHERE created_at >= '{week_start}'::timestamp
               AND created_at < '{week_end}'::timestamp + INTERVAL '1 days'
         GROUP BY lti_user_id, attempt_type;
    """

    connection = BaseHook.get_connection('conn_pg')
    with pg.connect(
        dbname='etl',
        sslmode='disable',
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE maks_khalilov_agr")
        cursor.execute(sql_query)
        conn.commit()

with DAG(
    dag_id='max_dag_les_9_jinja',
    tags=['max_khalilov', '7', 'jinja'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    catchup=False,
    user_defined_macros={
        'current_week_start': WeekTemplates.current_week_start,
        'current_week_end': WeekTemplates.current_week_end
    },
    render_template_as_native_obj=True
) as dag:
    
    start_dag = EmptyOperator(task_id='start_dag')
    end_dag = EmptyOperator(task_id='end_dag')

    combine_data_task = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data,
        op_kwargs={
            'week_start': '{{ current_week_start(ds) }}',  # Убран params.data_type
            'week_end': '{{ current_week_end(ds) }}',
            'execution_date': '{{ ds }}'
        }
    )

    upload_data_task = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_kwargs={
            'week_start': '{{ current_week_start(ds) }}',
            'week_end': '{{ current_week_end(ds) }}',
            'execution_date': '{{ ds }}'
        }
    )

    start_dag >> combine_data_task >> upload_data_task >> end_dag