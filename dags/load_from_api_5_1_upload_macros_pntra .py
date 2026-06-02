

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta


DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 2, 16),
}

class WeekTemplates:
    @staticmethod
    def current_week_start(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")
        current_week_start = (
            logical_dt -
            timedelta(days=logical_dt.weekday())
        )
        return current_week_start.strftime("%Y-%m-%d")
    @staticmethod
    def current_week_end(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")
        current_week_end = (
            logical_dt +
            timedelta(days=6 - logical_dt.weekday())
        )
        return current_week_end.strftime("%Y-%m-%d")



def combine_data(week_start: str, week_end : str,  **context):
    import psycopg2 as pg
    
    sql_query = f"""
        DELETE FROM admin_agg_table_pntra 
        WHERE date >= '{week_start}'::timestamp 
          AND date <= '{week_end}'::timestamp;

        INSERT INTO admin_agg_table_pntra
        SELECT lti_user_id,
               attempt_type,
               COUNT(1),
               COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_failed_count,
               -- Группируем всё под дату начала недели или ds, тут на ваше усмотрение
               '{week_start}'::timestamp
        FROM admin_table_pntra
        WHERE created_at >= '{week_start}'::timestamp
          AND created_at <= '{week_end}'::timestamp + INTERVAL '1 day' - INTERVAL '1 second'
        GROUP BY lti_user_id, attempt_type;
    """
    connection = BaseHook.get_connection('conn_pg')
    with pg.connect(
        dbname='neondb',
        sslmode='require',
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

def upload_data(week_start: str, week_end : str, **context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs
    # Выгружаем агрегированные данные за весь диапазон недели
    sql_query = f"""
        SELECT *
        FROM admin_agg_table_pntra
        WHERE date >= '{week_start}'::timestamp
          AND date <= '{week_end}'::timestamp;
    """

    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
        dbname='neondb',
        sslmode='require',
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
        Key = f"admin_weekly_{week_start}_to_{week_end}.csv"
    )

with DAG(
    dag_id='admin_agg_dag_macros_week_pntra',
    default_args=DEFAULT_ARGS,
    schedule='@daily',
    user_defined_macros={
    "current_week_start": WeekTemplates.current_week_start,
    "current_week_end": WeekTemplates.current_week_end,
    },
    render_template_as_native_obj=True,
    max_active_runs=1,
    max_active_tasks=1
    
) as dag:

    dag_start = EmptyOperator(
        task_id='dag_start'
    )
    dag_end = EmptyOperator(
        task_id='dag_end'
    )

    combine_data_task = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data,
        op_kwargs={
        'week_start': '{{ current_week_start(ds) }}',
        'week_end': '{{ current_week_end(ds) }}',
        }
    )
    upload_data_task = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
         op_kwargs={
        'week_start': '{{ current_week_start(ds) }}',
        'week_end': '{{ current_week_end(ds) }}',
        }
    )
  
    dag_start >> combine_data_task >> upload_data_task >> dag_end


