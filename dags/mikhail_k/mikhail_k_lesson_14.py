from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {
    'owner': 'mikhail_k',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 13),
}

API_URL = "https://b2b.itresume.ru/api/statistics"


def upload_data(**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = f"""
        SELECT * FROM mikhail_k_agg_table
        WHERE date >= '{context['ds']}'::timestamp
        AND date < '{context['ds']}'::timestamp + INTERVAL '7 days';
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
        config=Config(signature_version="s3v4"),
    )

    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=f"mikhail_k_{context['ds']}.csv"
    )



def combine_data(**context):
    import psycopg2 as pg

    sql_query = f"""
        INSERT INTO mikhail_k_agg_table
        SELECT lti_user_id,
            attempt_type,
            COUNT(1) AS cnt_attempt,
            COUNT(attempt_type) FILTER (WHERE is_correct) AS cnt_correct,
            '{context['ds']}'::timestamp AS date
        FROM mikhail_k_table 
        WHERE created_at >= '{context['ds']}'::timestamp
        AND created_at < '{context['ds']}'::timestamp + INTERVAL '7 days'
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
        cursor.execute(sql_query)
        conn.commit()

with DAG(
    dag_id="mikhail_k_lesson_9",
    tags=['mikhail_k'],
    schedule='@weekly',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    wait_3_msk = TimeDeltaSensor(
        task_id='wait_3_msk',
        delta=timedelta(hours=3),   # Сама дельта
        mode='reschedule',          # Чтобы таска не занимала слот, пока ждёт
        poke_interval=300,          # Время, через сколько секунд таска снова будет running
    )

    dag_sensor = ExternalTaskSensor(
        task_id='dag_sensor',
        external_dag_id = 'load_from_api_to_pg_with_operator',
        execution_delta=timedelta(minutes=0), 
        # Разница между запусками (расписаниями) дагов
        # Одинаковое расписание = 0
        # Даг, который ждём выполняется в прошлом, то > 0
        # Даг, который ждём выполняется в будущем, то < 0
        mode='reschedule',
        poke_interval=300,   
    )

    combine_data = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data,
    )

    upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
    )

    dag_start >> wait_3_msk >> dag_sensor >> \
        combine_data >> upload_data >> dag_end