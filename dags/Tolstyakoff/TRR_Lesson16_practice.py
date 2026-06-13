import psycopg2 as pg
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.sensors.base import BaseSensorOperator

from botocore.client import Config

#Классы с сенсорами

class MultiSqlSensor(BaseSensorOperator):
    template_fields = ('sql_list',)

    def __init__(self, sql_list, *args,**kwargs):
        super().__init__(*args,**kwargs)
        self.sql_list = sql_list

    def poke(self, context):
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
            tcp_user_timeout=600,
        ) as conn:
            cursor = conn.cursor()

            for i, sql in enumerate(self.sql_list):
                cursor.execute(sql)
                result = cursor.fetchone()

                if not result or result[0] == 0:
                    self.log.info(f'Query {i+1} returned no data')
                    return False
        return True
    
class SqlSensor(BaseSensorOperator):
    template_fields = ('sql',)

    def __init__(self, sql, *args,**kwargs):
        super().__init__(*args,**kwargs)
        self.sql = sql

    def poke(self, context):
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
            tcp_user_timeout=600,
        ) as conn:
            cursor = conn.cursor()
            self.log.info(self.sql)
            cursor.execute(self.sql)
            result = cursor.fetchone()
        if result[0] > 0:
            return True
        else:
            return False
        
DEFAULT_ARGS = {
    'owner': 'tolstyakoff',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 13),
}


def upload_data(**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = f"""
        SELECT * FROM tolstyakoff_agg_table_les9
        WHERE date >= '{context["ds"]}'::timestamp
        AND date < '{context["ds"]}'::timestamp + INTERVAL '1 days';
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
            tcp_user_timeout=600,
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
        quoting=csv.QUOTE_MINIMAL,
    )

    writer.writerows(data)
    file.seek(0)

    connection = BaseHook.get_connection('conn_s3')

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
        Key=f"tolstyakoff_16_practice_{context['ds']}.csv",
    )

def combine_data(**context):
    import psycopg2 as pg

    sql_query1 = "TRUNCATE tolstyakoff_agg_table_les9;"

    sql_query2 = f"""
        INSERT INTO tolstyakoff_agg_table_les9
        SELECT lti_user_id,
                attempt_type,
                COUNT(1),
                COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_failed_count,
                '{context["ds"]}'::timestamp
        FROM tolstyakoff_admin_table_8
        WHERE created_at >= '{context["ds"]}'::timestamp
        AND created_at < '{context["ds"]}'::timestamp + INTERVAL '1 days'
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
        tcp_user_timeout=600,
    ) as conn:
        cursor = conn.cursor()
        
        cursor.execute(sql_query1) #сначала очищаем таблицу 
        cursor.execute(sql_query2) #после залиаем в нее данные
        conn.commit()

with DAG(
    dag_id='tolstyakoff_lesson_16',
    tags=['16 practice', 'tolstyakoff'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    wait_3_msk = TimeDeltaSensor(
        task_id='wait_3_msk',
        delta=timedelta(hours=3),
        mode='reschedule',
        poke_interval=300
    )

    dag_sensor = ExternalTaskSensor(
        task_id='dag_sensor',
        external_dag_id='load_from_api_to_pg_with_operator',
        execution_delta=timedelta(minutes=0),
        mode='reschedule',
        poke_interval=300,
    )

    sql_sensor = MultiSqlSensor(
        task_id='sql_sensor',
        sql_list=[
            """
            SELECT COUNT(1)
            FROM tolstyakoff_admin_table_8
            WHERE created_at >= '{{ ds }}'::timestamp
            and created_at < '{{ ds }}'::timestamp + INTERVAL '1 days'
            """,
            """
            select count(1)
            FROM  tolstyakoff_api_data
            WHERE load_date = '2026-03-31'
            """
        ],
        mode='reschedule',
        poke_interval=300
    )

    combine_data = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data,
    )

    upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
    )

    dag_start >> wait_3_msk >> dag_sensor >> sql_sensor >> combine_data >> upload_data >> dag_end

