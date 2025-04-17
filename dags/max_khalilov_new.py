from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
import boto3
from botocore.client import Config
from io import BytesIO
import csv
import codecs
from typing import Optional, List, Any

# Параметры по умолчанию
DEFAULT_ARGS = {     
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 4, 12)
}

# Класс для вычисления начала и конца недели
class WeekTemplates:
    @staticmethod
    def current_week_start(date) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        current_week_start = logical_dt - timedelta(days=logical_dt.weekday())
        return current_week_start.strftime('%Y-%m-%d')

    @staticmethod
    def current_week_end(date) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        current_week_end = logical_dt + timedelta(days=6 - logical_dt.weekday())
        return current_week_end.strftime('%Y-%m-%d')

# Кастомный опера прим. для выполнения SQL-запросов
class DynamicSQLExecutorOperator(BaseOperator, LoggingMixin):
    template_fields = ('sql_query',)
    template_ext = ('.sql',)

    def __init__(
        self,
        conn_id: str,
        sql_query: str,
        database: Optional[str] = None,
        autocommit: bool = False,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_query = sql_query
        self.database = database
        self.autocommit = autocommit

    def execute(self, context: dict) -> None:
        hook = PostgresHook(postgres_conn_id=self.conn_id, schema=self.database)
        try:
            # Минимальное логирование: только начало выполнения
            self.log.info("Executing SQL query")
            hook.run(self.sql_query, autocommit=self.autocommit)
            self.log.info("Query executed successfully")
        except Exception as e:
            self.log.error(f"SQL query failed: {str(e)}")
            raise

# Кастомный оператор для выгрузки данных из PostgreSQL в S3
class PostgresToS3Operator(BaseOperator, LoggingMixin):
    template_fields = ('sql_query', 's3_key')
    
    def __init__(
        self,
        postgres_conn_id: str,
        s3_conn_id: str,
        sql_query: str,
        s3_bucket: str,
        s3_key: str,
        database: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.sql_query = sql_query
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.database = database

    def execute(self, context: dict) -> None:
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        try:
            self.log.info("Fetching data from PostgreSQL")
            data = pg_hook.get_records(self.sql_query)
            
            if not data:
                self.log.warning("No data returned from query")
                return
            
            # Запись данных в CSV
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
            
            # Подключение к S3
            s3_conn = BaseHook.get_connection(self.s3_conn_id)
            s3_client = boto3.client(
                's3',
                endpoint_url=s3_conn.host,
                aws_access_key_id=s3_conn.login,
                aws_secret_access_key=s3_conn.password,
                config=Config(signature_version='s3v4')
            )
            
            # Загрузка в S3
            self.log.info(f"Uploading to s3://{self.s3_bucket}/{self.s3_key}")
            s3_client.put_object(
                Body=file,
                Bucket=self.s3_bucket,
                Key=self.s3_key
            )
            self.log.info("Upload completed")
            
        except Exception as e:
            self.log.error(f"Operation failed: {str(e)}")
            raise

# Определение DAG
with DAG(
    dag_id='max_dag_les_9_jinja_modernized',
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

    combine_data_task = DynamicSQLExecutorOperator(
        task_id='combine_data',
        conn_id='conn_pg',
        database='etl',
        sql_query="""
            TRUNCATE TABLE maks_khalilov_agr;
            INSERT INTO maks_khalilov_agr
            SELECT lti_user_id,
                   attempt_type,
                   COUNT(1),
                   COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_fails_count,
                   '{{ ds }}'::timestamp
              FROM admin_table
             WHERE created_at >= '{{ current_week_start(ds) }}'::timestamp
                   AND created_at < '{{ current_week_end(ds) }}'::timestamp + INTERVAL '1 days'
             GROUP BY lti_user_id, attempt_type;
        """,
        autocommit=True
    )

    upload_data_task = PostgresToS3Operator(
        task_id='upload_data',
        postgres_conn_id='conn_pg',
        s3_conn_id='conn_s3',
        database='etl',
        sql_query="""
            SELECT * FROM admin_agg_table
            WHERE date >= '{{ current_week_start(ds) }}'::timestamp
                AND date < '{{ current_week_end(ds) }}'::timestamp + INTERVAL '1 days';
        """,
        s3_bucket='default-storage',
        s3_key="admin_{{ ds }}.csv"
    )

    start_dag >> combine_data_task >> upload_data_task >> end_dag