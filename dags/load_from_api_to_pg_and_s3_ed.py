"""DAG для организации ETL-процесса (API->PG->S3)"""

# Импорт библиотек для работы с AirFlow
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow import DAG

from datetime import datetime, timedelta

# Аргументы по умолчанию
DEFAULT_ARGS = {
    "owner": "ed",
    "retries": 2,
    "retry_delay": 600,
    "start_date": datetime(2025, 6, 22),
}

# Функция для загрузки данных по API в PG
def raw_data(**context):
    import requests
    import psycopg2 as pg
    
    API_URL = "https://b2b.itresume.ru/api/statistics"
    params = {
        "client": "Skillfactory",
        "client_key": "M2MGWS",
        "start": (datetime.strptime(context["ds"], "%Y-%m-%d") - timedelta(7)).strftime("%Y-%m-%d %H:%M:%S"),
        "end": f'{context["ds"]} 00:00:00',
    }
    response = requests.get(API_URL, params)
    data = response.json()

    connection = BaseHook.get_connection("conn_pg")
    with pg.connect(
        dbname="etl",
        sslmode="disable",
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:
        cursor = conn.cursor()

        for el in data:
            start_date = datetime.strptime(context["ds"], "%Y-%m-%d") - timedelta(7)
            end_date = datetime.strptime(f'{context["ds"]} 00:00:00', "%Y-%m-%d %H:%M:%S")
            created_at = el.get("created_at")
            created_at = datetime.strptime(created_at.split('.')[0], "%Y-%m-%d %H:%M:%S")
            if start_date <= created_at < end_date:
                lti_user_id = el.get("lti_user_id")
                created_at1 = el.get("created_at")
                cursor.execute("""SELECT 1 FROM raw_data_ed WHERE lti_user_id = %s AND created_at = %s""", (lti_user_id, created_at1))
                
                if not cursor.fetchone():
                    row = []
                    passback_params = eval(el.get("passback_params", "{}"))
                    row.append(el.get("lti_user_id"))
                    row.append(True if el.get("is_correct") == 1 else False)
                    row.append(el.get("attempt_type"))
                    row.append(el.get("created_at"))
                    row.append(passback_params.get("oauth_consumer_key"))
                    row.append(passback_params.get("lis_result_sourcedid"))
                    row.append(passback_params.get("lis_outcome_service_url"))

                    cursor.execute("""INSERT INTO raw_data_ed VALUES (%s, %s, %s, %s, %s, %s, %s)""", row)
        
        conn.commit()

def agg_data(**context):
    import psycopg2 as pg

    connection = BaseHook.get_connection("conn_pg")
    with pg.connect(
        dbname="etl",
        sslmode="disable",
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:
        
        cursor = conn.cursor()
        created_at2 = context["ds"]
        cursor.execute("""SELECT 1 FROM agg_data_ed WHERE date_start = %s""", (created_at2,))
        if not cursor.fetchone():
            sql_query = """
                INSERT INTO agg_data_ed
                SELECT
                    lti_user_id,
                    attempt_type,
                    %s AS date_start,
                    COUNT(is_correct) AS attempt_count,
                    SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) AS attempt_count_true,
                    SUM(CASE WHEN is_correct THEN 0 ELSE 1 END) AS attempt_count_false
                FROM
                    raw_data_ed
                WHERE
                    created_at >= %s::TIMESTAMP - INTERVAL '7 days' AND created_at < %s::TIMESTAMP
                GROUP BY
                    lti_user_id, attempt_type;
            """
            cursor.execute(sql_query, (context["ds"], context["ds"], context["ds"]))
        conn.commit()

def load_data(**context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query1 = """
    SELECT * FROM raw_data_ed WHERE created_at >= %s::TIMESTAMP - INTERVAL '7 days' AND created_at < %s::TIMESTAMP
"""
    sql_query2 = """
    SELECT * FROM agg_data_ed WHERE date_start = %s
"""
    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname="etl",
        sslmode="disable",
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        connect_timeout=600,
        keepalives_idle=600,
        tcp_user_timeout=600
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query1, (context["ds"], context["ds"]))
        data_raw = cursor.fetchall()
        cursor.execute(sql_query2, (context["ds"],))
        data_agg = cursor.fetchall()

    file1 = BytesIO()
    file2 = BytesIO()

    writer_wrapper = codecs.getwriter('utf-8')

    writer1 = csv.writer(
        writer_wrapper(file1),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )

    writer1.writerows(data_raw)
    file1.seek(0)

    writer2 = csv.writer(
        writer_wrapper(file2),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )

    writer2.writerows(data_agg)
    file2.seek(0)   

    connection = BaseHook.get_connection('conn_s3')
    s3_client = s3.client(
        's3',
        endpoint_url=connection.host,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        config=Config(signature_version="s3v4"),
    )

    s3_client.put_object(
        Body=file1,
        Bucket='default-storage',
        Key=f'ed_raw_{context["ds"]}.csv'
    )

    s3_client.put_object(
        Body=file2,
        Bucket='default-storage',
        Key=f'ed_agg_{context["ds"]}.csv'
    )

# Определение параметров DAGa
with DAG(
    dag_id="raw_api_data_and_data_ed",
    tags=["ed", "10"],
    schedule_interval="0 0 * * 1",
    default_args=DEFAULT_ARGS,
    catchup=True,
    )as dag:

    # Определение tasks
    start_dag = EmptyOperator(
        task_id="start_dag"
    )

    end_dag = EmptyOperator(
        task_id="end_dag"
    )

    raw_data = PythonOperator(
        task_id="raw_data",
        python_callable= raw_data,
    )

    agg_data = PythonOperator(
        task_id="agg_data",
        python_callable=agg_data,
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    start_dag >> raw_data >> agg_data >> load_data >> end_dag