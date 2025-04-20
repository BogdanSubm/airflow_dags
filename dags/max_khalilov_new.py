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
from typing import Optional

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

# Кастомный оператор для выполнения SQL-запросов
class DynamicSQLExecutorOperator(BaseOperator, LoggingMixin):
    template_fields = ('sql_query',)
    template_ext = ('.sql',)

    def __init__(
        self,
        conn_id: str,  # Ссылка на conn_pg
        sql_query: str, # SQL-запрос
        database: Optional[str] = None, # Имя базы данных
        autocommit: bool = False, # Флаг для автокоммита
        *args, # Аргументы
        **kwargs # Ключевые аргументы
    ):
        super().__init__(*args, **kwargs) # Вызываем конструктор родительского класса (BaseOperator)
        self.conn_id = conn_id # Ссылка на conn_pg
        self.sql_query = sql_query # SQL-запрос
        self.database = database # Имя базы данных
        self.autocommit = autocommit # Флаг для автокоммита

    def execute(self, context: dict) -> None: # Метод для выполнения SQL-запроса
        hook = PostgresHook(postgres_conn_id=self.conn_id, schema=self.database) # Создаем hook для conn_pg
        try:
            hook.run(self.sql_query, autocommit=self.autocommit) # Выполняем SQL-запрос
            self.log.info("Query executed successfully") # Логируем успешность выполнения запроса
        except Exception as e:
            self.log.error(f"SQL query failed: {str(e)}") # Логируем ошибку 
            raise

# Кастомный оператор для выгрузки данных из PostgreSQL в S3
class PostgresToS3Operator(BaseOperator, LoggingMixin):
    template_fields = ('sql_query', 's3_key') # Поля для шаблонов       
    
    def __init__(
        self,
        conn_id: str,  # Ссылка на conn_pg
        s3_conn_id: str,  # Ссылка на conn_s3
        sql_query: str, # SQL-запрос
        s3_bucket: str, # Имя бакета
        s3_key: str, # Имя файла
        database: Optional[str] = None, # Имя базы данных
        *args, # Аргументы
        **kwargs # Ключевые аргументы
    ):
        super().__init__(*args, **kwargs) # Вызываем конструктор родительского класса (BaseOperator)
        self.conn_id = conn_id # Ссылка на conn_pg
        self.s3_conn_id = s3_conn_id # Ссылка на conn_s3
        self.sql_query = sql_query # SQL-запрос
        self.s3_bucket = s3_bucket # Имя бакета
        self.s3_key = s3_key # Имя файла
        self.database = database # Имя базы данных

    def execute(self, context: dict) -> None: # Метод для выгрузки данных из PostgreSQL в S3
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id, schema=self.database) # Создаем hook для conn_pg
        data = pg_hook.get_records(self.sql_query) # Получаем данные из PostgreSQL
        if not data:
            self.log.debug("No data returned from query") # Логируем отсутствие данных
            return
        file = BytesIO() # Создаем BytesIO для хранения данных
        writer_wrapper = codecs.getwriter('utf-8') # Создаем writer_wrapper для кодировки UTF-8
        writer = csv.writer(
            writer_wrapper(file),
            delimiter='\t', # Разделитель
            lineterminator='\n', # Символ конца строки
            quotechar='"', # Символ для кавычек
            quoting=csv.QUOTE_MINIMAL # Способ кавычек
        )
        writer.writerows(data) # Записываем данные в файл
        file.seek(0) # Сдвигаем указатель в начало файла
        s3_conn = BaseHook.get_connection(self.s3_conn_id) # Получаем соединение с conn_s3
        s3_client = boto3.client(
            's3',
            endpoint_url=s3_conn.host, # Хост
            aws_access_key_id=s3_conn.login, # Логин
            aws_secret_access_key=s3_conn.password, # Пароль
            config=Config(signature_version='s3v4') # Версия подписи
        )
        s3_client.put_object(
            Body=file, # Тело файла
            Bucket=self.s3_bucket, # Имя бакета
            Key=self.s3_key # Имя файла
        )
        self.log.info("Upload completed") # Логируем завершение выгрузки

# Определение DAG
with DAG(
    dag_id='max_dag_les_9_jinja_modernized', # Имя DAG
    tags=['max_khalilov', '7', 'jinja'], # Теги
    schedule='@daily', # Расписание
    default_args=DEFAULT_ARGS, # Аргументы по умолчанию
    max_active_runs=1, # Максимальное количество запусков DAG
    max_active_tasks=1, # Максимальное количество задач
    catchup=False, # Отключаем catchup
    user_defined_macros={
        'current_week_start': WeekTemplates.current_week_start, # Макрос для начала недели
        'current_week_end': WeekTemplates.current_week_end # Макрос для конца недели
    },
    render_template_as_native_obj=True # Отключаем рендеринг шаблонов как строк
) as dag:
    
    start_dag = EmptyOperator(task_id='start_dag') # Начало DAG
    end_dag = EmptyOperator(task_id='end_dag') # Конец DAG

    combine_data_task = DynamicSQLExecutorOperator(
        task_id='combine_data', # Имя задачи    
        conn_id='conn_pg', # Ссылка на conn_pg
        database='etl', # Имя базы данных
        sql_query="""
            TRUNCATE TABLE maks_khalilov_agr;
            INSERT INTO maks_khalilov_agr
            SELECT lti_user_id,
                   attempt_type,
                   COUNT(1),
                   COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_fails_count,
                   COUNT(*) AS cnt_attempts
              FROM admin_table
             WHERE created_at >= '{{ current_week_start(ds) }}'::timestamp
                   AND created_at < '{{ current_week_end(ds) }}'::timestamp + INTERVAL '1 days'
             GROUP BY lti_user_id, attempt_type;
        """,
        autocommit=True
    )

    upload_data_task = PostgresToS3Operator( 
        task_id='upload_data', # Имя задачи
        conn_id='conn_pg', # Ссылка на conn_pg
        s3_conn_id='conn_s3', # Ссылка на conn_s3
        database='etl', # Имя базы данных
        sql_query="""
            SELECT * FROM admin_agg_table
            WHERE date >= '{{ current_week_start(ds) }}'::timestamp
                AND date < '{{ current_week_end(ds) }}'::timestamp + INTERVAL '1 days';
        """,
        s3_bucket='default-storage', # Имя бакета
        s3_key="admin_{{ ds }}.csv" # Имя файла
    )

    start_dag >> combine_data_task >> upload_data_task >> end_dag