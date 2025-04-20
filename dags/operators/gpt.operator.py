from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin
from typing import Optional, List, Any

class DynamicSQLExecutorOperator(BaseOperator, LoggingMixin):
    """
    Оператор для выполнения динамических SQL-запросов с использованием Jinja-шаблонов.
    Поддерживает подключение к базе данных через Airflow-хук и сохранение результатов в XCom.
    
    :param conn_id: ID подключения к базе данных (например, 'my_postgres_conn').
    :param sql_query: SQL-запрос, который может содержать Jinja-шаблоны (например, '{{ ds }}').
    :param xcom_push_key: Ключ для сохранения результатов в XCom (опционально).
    :param database: Имя базы данных (опционально, если не указано в conn_id).
    """
    
    template_fields = ('sql_query',)
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        sql_query: str,
        xcom_push_key: Optional[str] = None,
        database: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_query = sql_query
        self.xcom_push_key = xcom_push_key
        self.database = database

    def execute(self, context: dict) -> Optional[List[Any]]:
        """
        Выполняет SQL-запрос и возвращает результаты, если они есть.
        Сохраняет результаты в XCom, если указан xcom_push_key.
        """
        # Инициализация хука для подключения к базе данных
        hook = PostgresHook(postgres_conn_id=self.conn_id, schema=self.database)
        
        # Логирование начала выполнения
        self.log.info(f"Executing SQL query: {self.sql_query}")
        
        try:
            # Выполнение запроса
            result = hook.get_records(self.sql_query)
            
            # Логирование результатов
            if result:
                self.log.info(f"Query returned {len(result)} rows")
            else:
                self.log.info("Query returned no rows")
                
            # Сохранение результатов в XCom, если указан ключ
            if self.xcom_push_key:
                context['ti'].xcom_push(key=self.xcom_push_key, value=result)
                self.log.info(f"Results pushed to XCom with key: {self.xcom_push_key}")
                
            return result
            
        except Exception as e:
            self.log.error(f"Error executing SQL query: {str(e)}")
            raise

# Пример использования в DAG
from airflow import DAG
from datetime import datetime

with DAG(
    dag_id='dynamic_sql_dag',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    # Задача для выполнения SQL-запроса
    execute_sql = DynamicSQLExecutorOperator(
        task_id='execute_dynamic_sql',
        conn_id='my_postgres_conn',
        sql_query="SELECT * FROM my_table WHERE date = '{{ ds }}'",
        xcom_push_key='sql_results'
    )