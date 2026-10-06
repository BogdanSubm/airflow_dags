from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pendulum
from juventa.custom_postgresoperator import JCustomPostgresOperator

DEFAULT_ARGS = {
    'owner': 'juventa',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': pendulum.datetime(2024, 10, 23, tz="UTC")
}

API_URL = "https://b2b.itresume.ru/api/statistics"

#создаю свои Templates!
class WeekTemplates:
    @staticmethod
    #возвращает начало недели (строка)
    def current_week_start(date) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        current_week_start = logical_dt - timedelta(days=logical_dt.weekday())
        return current_week_start.strftime('%Y-%m-%d')

    @staticmethod
    #возвращает конец недели (строка)
    def current_week_end(date) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        current_week_end = logical_dt + timedelta(days=6 - logical_dt.weekday())
        return current_week_end.strftime('%Y-%m-%d')

#загружаем данные из api
def load_from_api(week_start:str, week_end:str, **context):
    import requests
    import pendulum
    import psycopg2 as pg
    import ast

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': week_start,
        'end': week_end
    }
    response = requests.get(API_URL, params=payload, timeout=60)
    response.raise_for_status()
    data = response.json()

    print(f"Запрос с {week_start} по {week_end}, получено: {len(data)} записей")

    #данные которые ранее получили из API
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
        delete_query = '''
            DELETE FROM juventa_raw_table
            where created_at >= %s::timestamp
              and created_at < %s::timestamp + interval '1 day'
            '''
        cursor.execute(delete_query,(week_start,week_end))
        print(f"Удалены данные в БД PostgreSQL за период с {week_start} по {week_end}")

        for el in data:
            row = []
            passback_params = ast.literal_eval(el.get('passback_params') or '{}')
            row.append(el.get('lti_user_id'))
            row.append(True if el.get('is_correct') == 1 else False)
            row.append(el.get('attempt_type'))
            row.append(el.get('created_at'))
            row.append(passback_params.get('oauth_consumer_key'))
            row.append(passback_params.get('lis_result_sourcedid'))
            row.append(passback_params.get('lis_outcome_service_url'))

            cursor.execute("INSERT INTO juventa_raw_table VALUES(%s, %s, %s, %s, %s, %s, %s)", row)
        conn.commit()
        print('Сырые данные сохранены в БД PostgreSQL')

def save_raw_to_minio(week_start:str, week_end:str, **context):
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
            SELECT * from juventa_raw_table
            WHERE created_at >= '{week_start}'::timestamp
              and created_at < '{week_end}'::timestamp + interval '1 days'
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
        #Key=f"juventa_{start}_{end}.csv"
        Key=f"juventa/raw/j_{week_start}-{week_end}.csv"
    )

    print(f"Файл juventa/raw/j_{week_start}_{week_end}.csv загружен")

def aggr_data(week_start:str, week_end:str, **context):
    import psycopg2 as pg


    sql_query = f"""
        INSERT INTO juventa_aggr_table
        SELECT
            %s::date,
            %s::date,
            COUNT(*) AS total_attempts,
            COUNT(DISTINCT lti_user_id) AS unique_students,
            COUNT(DISTINCT oauth_consumer_key) AS unique_courses,
            COUNT(*) FILTER (WHERE is_correct = true) AS correct_attempts,
            ROUND(
              COUNT(*) FILTER (WHERE is_correct = true)::numeric / NULLIF(COUNT(*), 0) * 100.0,
              2
            ) AS overall_success_rate_pct,
            MIN(created_at) AS earliest_record,
            MAX(created_at) AS latest_record
        FROM juventa_raw_table
        where created_at >= '{week_start}'::timestamp
              and created_at < '{week_end}'::timestamp + interval '1 days'
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

        cursor.execute('''
                    DELETE FROM juventa_aggr_table
                    WHERE period_start = %s::date AND period_end = %s::date
                    ''', (week_start, week_end))

        print(f"Удалены агр.данные в БД PostgreSQL за период с {week_start} по {week_end}")
        print(sql_query)

        cursor.execute(sql_query,(week_start, week_end))
        conn.commit()

def save_agg_to_minio(week_start:str, week_end:str, **context):
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
            SELECT * from juventa_aggr_table
            WHERE period_start= '{week_start}'::date
                AND period_end = '{week_end}'::date
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
        print(sql_query)
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
        #Key=f"juventa_{period_start}_{period_end}.csv"
        Key=f"juventa/aggr/j_{week_start}-{week_end}.csv"
    )

    print(f"Файл juventa/aggr/j_{week_start}-{week_end}.csv загружен")



with DAG(
    dag_id='juventa_13_homework',
    #schedule='0 0 * * 1',  # Понедельник в 00:00 UTC
    schedule='@weekly',
    #catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    #Регистрирую свои templates
    user_defined_macros={
        "current_week_start": WeekTemplates.current_week_start,
        "current_week_end": WeekTemplates.current_week_end
    },
    render_template_as_native_obj=True

) as dag:
    dag_start = EmptyOperator(task_id='dag_start')

    dag_end = EmptyOperator(task_id='dag_end')

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_from_api,
        op_kwargs={
            'week_start': '{{ current_week_start(ds) }}',
            'week_end': '{{ current_week_end(ds) }}',
        }
    )

    raw_export_task = PythonOperator(
        task_id='raw_export_task',
        python_callable=save_raw_to_minio,
        op_kwargs={
            'week_start': '{{ current_week_start(ds) }}',
            'week_end': '{{ current_week_end(ds) }}',
        }
    )

    #aggregate_task = PythonOperator(
#        task_id='aggregate_task',
#        python_callable=aggr_data,
#        op_kwargs={
#            'week_start': '{{ current_week_start(ds) }}',
#            'week_end': '{{ current_week_end(ds) }}',
#        }
#    )
    aggregate_task_delete = JCustomPostgresOperator(
        task_id='aggregate_task_delete',
        postgres_conn_id='conn_pg',
        dbname='etl',
        sql_query="""
            DELETE FROM juventa_aggr_table
            WHERE period_start = %s::date AND period_end = %s::date
        """,
        parameters=(
            '{{ current_week_start(ds) }}',
            '{{ current_week_end(ds) }}'
        )
    )

    aggregate_task_insert = JCustomPostgresOperator(
        task_id='aggregate_task_insert',
        postgres_conn_id='conn_pg',
        dbname='etl',
        sql_query="""
        INSERT INTO juventa_aggr_table
        SELECT
            %s::date,
            %s::date,
            COUNT(*) AS total_attempts,
            COUNT(DISTINCT lti_user_id) AS unique_students,
            COUNT(DISTINCT oauth_consumer_key) AS unique_courses,
            COUNT(*) FILTER (WHERE is_correct = true) AS correct_attempts,
            ROUND(
              COUNT(*) FILTER (WHERE is_correct = true)::numeric / NULLIF(COUNT(*), 0) * 100.0,
              2
            ) AS overall_success_rate_pct,
            MIN(created_at) AS earliest_record,
            MAX(created_at) AS latest_record
        FROM juventa_raw_table
        where created_at >=  %s::timestamp
              and created_at < %s::timestamp + interval '1 days'
        """,
        parameters=(
            '{{ current_week_start(ds) }}',
            '{{ current_week_end(ds) }}',
            '{{ current_week_start(ds) }}',
            '{{ current_week_end(ds) }}',
        )
    )

    agg_export_task = PythonOperator(
        task_id='agg_export_task',
        python_callable=save_agg_to_minio,
        op_kwargs={
            'week_start': '{{ current_week_start(ds) }}',
            'week_end': '{{ current_week_end(ds) }}',
        }
    )

    dag_start >> load_task

    load_task >> raw_export_task >> dag_end
    load_task >> [aggregate_task_delete, aggregate_task_insert] >> agg_export_task >> dag_end