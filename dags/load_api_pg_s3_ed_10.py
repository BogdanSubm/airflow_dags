# Импорт библиотек
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

from datetime import datetime, timedelta
from calendar import monthrange

# Аргументы по умолчанию
DEFAULT_ARGS = {
    "owner": "ed",
    "retries": 2,
    "retry_delay": 600,
    "start_date": datetime(2025, 6, 29),
}

# Параметры Jinja
class MonthTemplates:
    @staticmethod
    def current_month_start(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")

        current_month_start = logical_dt.replace(day=1)

        return current_month_start.strftime("%Y-%m-%d")
    
    @staticmethod
    def current_month_end(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")

        current_year = logical_dt.year
        current_month = logical_dt.month
        day_week, last_day_month = monthrange(current_year, current_month)
        current_month_end = datetime(current_year, current_month, last_day_month)

        return current_month_end.strftime("%Y-%m-%d")

# Функции для PythonOperator
def raw_data(month_start: str, month_end: str, **context):
    import requests
    import psycopg2 as pg

    URL = 'https://b2b.itresume.ru/api/statistics'
    PARAMS = {
        'client': 'Skillfactory',
        'client_key': 'M2MGWS',
        'start': month_start,
        'end': month_end
    }
    response = requests.get(URL, params=PARAMS)
    data = response.json()

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

        for el in data:
            current_date = datetime.strptime(el.get('created_at'), "%Y-%m-%d %H:%M:%S.%f")
            current_user = el.get('lti_user_id')
            cursor.execute("""SELECT 1 FROM raw_data_ed WHERE lti_user_id = %s AND created_at = %s""", (current_user, current_date))
            if not cursor.fetchone():
                row = []
                passback_params = eval(el.get("passback_params", "{}"))
                row.append(el.get('lti_user_id'))
                row.append(True if el.get('is_correct') == 1 else False)
                row.append(el.get('attempt_type'))
                row.append(el.get('created_at'))
                row.append(passback_params.get('oauth_consumer_key'))
                row.append(passback_params.get('lis_result_sourcedid'))
                row.append(passback_params.get('lis_outcome_service_url'))
                cursor.execute("""INSERT INTO raw_data_ed VALUES (%s, %s, %s, %s, %s, %s, %s)""", row)
        
        conn.commit()

def upload_data(month_start: str, month_end: str, **context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = """
        SELECT * FROM raw_data_ed;
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

    filename = datetime.strptime(month_start, "%Y-%m-%d")
    filename_year = filename.year
    filename_month = filename.month
   
    s3_client.put_object(
        Body=file,
        Bucket='default_storage',
        Key=f"ed_{filename_year}-{filename_month}.csv"
    )

# Параметры DAG
with DAG(
    dag_id='load_api_pg_s3_ed_10',
    tags=['ed', '10'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    catchup=True,
    max_active_runs=1,
    max_active_tasks=1,
    user_defined_macros={
        'current_month_start': MonthTemplates.current_month_start,
        'current_month_end': MonthTemplates.current_month_end,
    },
    render_template_as_native_obj=True
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    raw_data = PythonOperator(
        task_id='raw_data',
        python_callable=raw_data,
        op_kwargs={
            'month_start': '{{ current_month_start(ds) }}',
            'month_end': '{{ current_month_end(ds) }}'
        }
    )

    upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_kwargs={
            'month_start': '{{ current_month_start(ds) }}',
            'month_end': '{{ current_month_end(ds) }}'
        }
    )

    dag_start >> raw_data >> upload_data >> dag_end