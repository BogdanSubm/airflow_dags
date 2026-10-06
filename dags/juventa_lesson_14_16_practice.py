from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.sensors.time_delta import TimeDeltaSensor

from juventa.jsql_sensor_new import JSqlSensor1

DEFAULT_ARGS = {
    'owner': 'juventa',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 10, 23)
}


def combine_data(**context):
    import psycopg2 as pg

    sql_query = f"""
        INSERT INTO juventa_agg_table
        SELECT 
           lti_user_id,
           attempt_type,
           COUNT(1),
           COUNT(case when is_correct then null else 1 end) as attempt_failed_count,
           '{context['ds']}'::timestamp
        FROM juventa_raw_month_table
        where created_at >= '{context['ds']}'::timestamp
              and created_at < '{context['ds']}'::timestamp + interval '1 days'
        group by lti_user_id, attempt_type    
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


def upload_data(**context):
    import psycopg2 as pg
    # байтовый объект
    from io import BytesIO
    import csv
    # библиотека для общения с объектными хранилищами
    import boto3 as s3
    # для подключения к minio
    from botocore.client import Config
    import codecs

    sql_query = f"""
        SELECT * from juventa_agg_table
        WHERE date >= '{context['ds']}'::timestamp
            AND date < '{context['ds']}'::timestamp + interval '1 days'
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
        cursor.execute(sql_query)  # выполняем запрос
        data = cursor.fetchall()  # возвращает структуру данных - массив в массивах

    file = BytesIO()  # файловый объект
    writer_wrapper = codecs.getwriter('utf-8')

    writer = csv.writer(
        writer_wrapper(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
    )

    writer.writerows(data)  # записываем данные из селекта в файл
    file.seek(0)  # возвращаем курсор ффайла в начало

    connection = BaseHook.get_connection('conn_s3')  # для подключения к объектному хранидищу

    s3_client = s3.client(
        's3',
        endpoint_url=connection.host,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version='s3v4'),
    )

    s3_client.put_object(
        Body=file,
        Bucket='default-storage',
        Key=f"juventa_{context['ds']}.csv",
    )


with DAG(
        dag_id='juventa_14_16_homework',
        schedule='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        max_active_tasks=1
) as dag:
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    sql_sensor = JSqlSensor1(
        task_id='sql_sensor',
        # sql="""
        #                    SELECT COUNT(1)
        #                    FROM juventa_raw_month_table
        #                    WHERE created_at >= '{{ ds }}'::timestamp
        #                    AND created_at < '{{ ds }}'::timestamp + interval '1 days'
        #                  """,
        sql=["""
                SELECT COUNT(1)
                FROM juventa_raw_month_table
                WHERE created_at >= '{{ ds }}'::timestamp 
                AND created_at < '{{ ds }}'::timestamp  + interval '1 days'
              """,
             """
               SELECT COUNT(1)
               FROM juventa_raw_table               
               WHERE created_at >= '{{ ds }}'::timestamp
                AND created_at < '{{ ds }}'::timestamp  + interval '1 days'
             """
             ],
        mode='reschedule',
        poke_interval=300,  #через сколько ещё раз запускать
        timeout=7200,       #через сколько = failed
    )

    dag_start >> sql_sensor >> dag_end