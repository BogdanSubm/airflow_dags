from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pendulum


DEFAULT_ARGS = {
    'owner': 'juventa',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': pendulum.datetime(2024, 9, 23, tz="UTC")
}

API_URL = "https://b2b.itresume.ru/api/statistics"

#загружаем данные из api
def load_from_api(**context):
    import requests
    import pendulum
    import psycopg2 as pg
    import ast

    period_start = context["data_interval_start"].in_timezone("UTC")
    period_end = context["data_interval_end"].in_timezone("UTC")  #следующий понедельник

    payload = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': period_start.to_date_string(),
        'end': period_end.subtract(days=1).to_date_string() #по воскресенье
    }
    response = requests.get(API_URL, params=payload, timeout=60)
    response.raise_for_status()
    data = response.json()

    #так лучше не делать если много данных
    #context['ti'].xcom_push(key='raw_data', value=data)
    context['ti'].xcom_push(key='period_start', value=period_start)
    context['ti'].xcom_push(key='period_end', value=period_end)
    print(f"Запрос с {period_start} по {period_end}, получено: {len(data)} записей")

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
        cursor.execute('''
            DELETE FROM juventa_raw_table
            WHERE created_at >= %s AND created_at < %s
            ''', (period_start, period_end))
        print(f"Удалены данные в БД PostgreSQL за период с {period_start} по {period_end}")

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

def save_raw_to_minio(**context):
    import psycopg2 as pg
    # байтовый объект
    from io import BytesIO
    import csv
    # библиотека для общения с объектными хранилищами
    import boto3 as s3
    # для подключения к minio
    from botocore.client import Config
    import codecs

    # данные которые ранее получили из API
    ti = context['ti']
    #data = ti.xcom_pull(key='raw_data')
    period_start = ti.xcom_pull(key='period_start')
    period_end = ti.xcom_pull(key='period_end')
    sql_query = f"""
            SELECT * from juventa_raw_table
            WHERE created_at >= '{period_start}'::timestamp
                AND created_at < '{period_end}'::timestamp 
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
        Key=f"juventa/raw/j_{period_start:%Y-%m-%d}-{period_end:%Y-%m-%d}.csv"
    )

    print(f"Файл juventa/raw/j_{period_start:%Y-%m-%d}_{period_end:%Y-%m-%d}.csv загружен")

def aggr_data(**context):
    import psycopg2 as pg
    # данные которые ранее получили из API
    ti = context['ti']

    period_start = ti.xcom_pull(key='period_start')
    period_end = ti.xcom_pull(key='period_end')

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
        where created_at >= '{period_start}'::timestamp
              and created_at < '{period_end}'::timestamp 
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
                    ''', (period_start, period_end))

        print(f"Удалены агр.данные в БД PostgreSQL за период с {period_start} по {period_end}")
        print(sql_query)

        cursor.execute(sql_query,(period_start, period_end))
        conn.commit()

def save_agg_to_minio(**context):
    import psycopg2 as pg
    # байтовый объект
    from io import BytesIO
    import csv
    # библиотека для общения с объектными хранилищами
    import boto3 as s3
    # для подключения к minio
    from botocore.client import Config
    import codecs

    # данные которые ранее получили из API
    ti = context['ti']
    #data = ti.xcom_pull(key='raw_data')
    period_start = ti.xcom_pull(key='period_start')
    period_end = ti.xcom_pull(key='period_end')
    sql_query = f"""
            SELECT * from juventa_aggr_table
            WHERE period_start= '{period_start}'::date
                AND period_end = '{period_end}'::date 
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
        Key=f"juventa/aggr/j_{period_start:%Y-%m-%d}-{period_end:%Y-%m-%d}.csv"
    )

    print(f"Файл juventa/aggr/j_{period_start:%Y-%m-%d}-{period_end:%Y-%m-%dd}.csv загружен")



with DAG(
    dag_id='juventa_8_9_practice',
    schedule='0 0 * * 1',  # Понедельник в 00:00 UTC
    #catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=2
) as dag:
    dag_start = EmptyOperator(task_id='dag_start')

    dag_end = EmptyOperator(task_id='dag_end')

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_from_api
    )

    raw_export_task = PythonOperator(
        task_id='raw_export_task',
        python_callable=save_raw_to_minio
    )

    aggregate_task = PythonOperator(
        task_id='aggregate_task',
        python_callable=aggr_data
    )
    agg_export_task = PythonOperator(
        task_id='agg_export_task',
        python_callable=save_agg_to_minio
    )

    dag_start >> load_task

    load_task >> raw_export_task >> dag_end
    load_task >> aggregate_task >> agg_export_task >> dag_end