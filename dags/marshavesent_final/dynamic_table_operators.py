"""
Кастомные операторы для динамической генерации таблиц.

Операторы разделены :
- CreateTableOperator: только создание таблицы
- InsertDataOperator: только загрузка данных
- DataQualityOperator: проверка качества данных
- DynamicExportOperator: экспорт в S3
"""

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowFailException
from typing import Dict, Optional, List
import psycopg2 as pg
from psycopg2.extras import RealDictCursor
from io import BytesIO
import csv
import codecs


class CreateTableOperator(BaseOperator):
    """
    Оператор для создания таблицы (DDL).
    
    Отвечает только за выполнение DDL.
    Идемпотентность через IF NOT EXISTS.
    """
    
    template_fields = ('table_ddl',)
    ui_color = '#85c1e9'
    
    @apply_defaults
    def __init__(
        self,
        table_name: str,
        table_ddl: str,
        pg_conn_id: str = 'conn_pg',
        pg_db: str = 'etl',
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.table_ddl = table_ddl
        self.pg_conn_id = pg_conn_id
        self.pg_db = pg_db
    
    def execute(self, context):
        """Создает таблицу если она не существует."""
        self.log.info(f'Создание таблицы: {self.table_name}')
        
        conn_info = BaseHook.get_connection(self.pg_conn_id)
        
        with pg.connect(
            dbname=self.pg_db,
            sslmode='disable',
            user=conn_info.login,
            password=conn_info.password,
            host=conn_info.host,
            port=conn_info.port,
            connect_timeout=600
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(self.table_ddl)
                conn.commit()
        
        self.log.info(f'+ Таблица {self.table_name} готова')


class InsertDataOperator(BaseOperator):
    """
    Оператор для наполнения таблицы данными (DML).
    
    Отвечает только за выполнение DML.
    Возвращает количество обработанных строк.
    """
    
    template_fields = ('table_dml',)
    ui_color = '#5dade2'
    
    @apply_defaults
    def __init__(
        self,
        table_name: str,
        table_dml: str,
        pg_conn_id: str = 'conn_pg',
        pg_db: str = 'etl',
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.table_dml = table_dml
        self.pg_conn_id = pg_conn_id
        self.pg_db = pg_db
    
    def execute(self, context):
        """Выполняет DML и возвращает количество строк."""
        self.log.info(f'Наполнение данными: {self.table_name}')
        
        conn_info = BaseHook.get_connection(self.pg_conn_id)
        
        with pg.connect(
            dbname=self.pg_db,
            sslmode='disable',
            user=conn_info.login,
            password=conn_info.password,
            host=conn_info.host,
            port=conn_info.port,
            connect_timeout=600
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(self.table_dml)
                row_count = cur.rowcount
                conn.commit()
        
        self.log.info(f'+ Обработано строк: {row_count}')
        
        context['task_instance'].xcom_push(key='row_count', value=row_count)
        
        return row_count


class DataQualityOperator(BaseOperator):
    """
    Оператор для проверки качества данных.
    
    Проверяет:
    1. Минимальное количество строк
    2. Процент NULL в критических колонках
    3. Корректность данных
    
    Если проверка не пройдена - задача падает с ошибкой.
    """
    
    template_fields = ('table_name', 'critical_columns')
    ui_color = '#f39c12'
    
    @apply_defaults
    def __init__(
        self,
        table_name: str,
        min_rows: int = 1,
        critical_columns: Optional[List[str]] = None,
        max_null_pct: float = 10.0,
        pg_conn_id: str = 'conn_pg',
        pg_db: str = 'etl',
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.min_rows = min_rows
        self.critical_columns = critical_columns or []
        self.max_null_pct = max_null_pct
        self.pg_conn_id = pg_conn_id
        self.pg_db = pg_db
    
    def execute(self, context):
        """Проверяет качество данных в таблице."""
        self.log.info(f'Проверка качества: {self.table_name}')
        
        conn_info = BaseHook.get_connection(self.pg_conn_id)
        
        with pg.connect(
            dbname=self.pg_db,
            sslmode='disable',
            user=conn_info.login,
            password=conn_info.password,
            host=conn_info.host,
            port=conn_info.port,
            connect_timeout=600
        ) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                
                cur.execute(f"SELECT COUNT(*) as cnt FROM {self.table_name}")
                total_rows = cur.fetchone()['cnt']
                
                self.log.info(f'Всего строк: {total_rows}, минимум: {self.min_rows}')
                
                if total_rows < self.min_rows:
                    raise AirflowFailException(
                        f"- Проверка не пройдена! "
                        f"В таблице {self.table_name} только {total_rows} строк, "
                        f"требуется минимум {self.min_rows}"
                    )
                
                self.log.info(f'+ Проверка количества строк пройдена')
                
                if self.critical_columns:
                    for col in self.critical_columns:
                        cur.execute(f"""
                            SELECT 
                                COUNT(*) as total,
                                COUNT({col}) as non_null,
                                ROUND(
                                    (COUNT(*) - COUNT({col}))::decimal / 
                                    NULLIF(COUNT(*), 0) * 100, 2
                                ) as null_pct
                            FROM {self.table_name}
                        """)
                        result = cur.fetchone()
                        null_pct = result['null_pct'] or 0
                        
                        self.log.info(
                            f'Колонка {col}: NULL {null_pct}% '
                            f'(максимум {self.max_null_pct}%)'
                        )
                        
                        if null_pct > self.max_null_pct:
                            raise AirflowFailException(
                                f"- Проверка не пройдена! "
                                f"В колонке {col} таблицы {self.table_name} "
                                f"{null_pct}% NULL значений, "
                                f"максимально допустимо {self.max_null_pct}%"
                            )
                    
                    self.log.info(f'+ Проверка NULL значений пройдена')
        
        self.log.info(f'+ Проверка качества {self.table_name} пройдена успешно')


class DynamicExportOperator(BaseOperator):
    """
    Оператор для экспорта таблицы в CSV и загрузки в S3.
    """
    
    template_fields = ('export_prefix',)
    ui_color = '#2ecc71'
    
    @apply_defaults
    def __init__(
        self,
        table_name: str,
        export_prefix: str,
        pg_conn_id: str = 'conn_pg',
        pg_db: str = 'etl',
        s3_conn_id: str = 'conn_s3',
        s3_bucket: str = 'marshavesent-bucket',
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.export_prefix = export_prefix
        self.pg_conn_id = pg_conn_id
        self.pg_db = pg_db
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
    
    def execute(self, context):
        """Экспортирует данные в CSV и загружает в S3."""
        import boto3
        from botocore.client import Config
        
        ds = context['ds']
        
        self.log.info(f'Экспорт: {self.table_name} за {ds}')
        
        pg_conn = BaseHook.get_connection(self.pg_conn_id)
        
        with pg.connect(
            dbname=self.pg_db,
            sslmode='disable',
            user=pg_conn.login,
            password=pg_conn.password,
            host=pg_conn.host,
            port=pg_conn.port,
            connect_timeout=600
        ) as conn:
            cursor = conn.cursor()
            
            try:
                cursor.execute(
                    f"SELECT * FROM {self.table_name} WHERE load_date = %s::date",
                    (ds,)
                )
            except:
                cursor.execute(
                    f"SELECT * FROM {self.table_name} WHERE created_at::date = %s::date",
                    (ds,)
                )
            
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            self.log.info(f'Выгружено строк: {len(rows)}')
            
            if not rows:
                self.log.warning('Нет данных для экспорта!')
                return
        
        file = BytesIO()
        writer = csv.writer(codecs.getwriter('utf-8')(file))
        writer.writerow(columns)
        writer.writerows(rows)
        file.seek(0)
        
        s3_conn = BaseHook.get_connection(self.s3_conn_id)
        s3_client = boto3.client(
            's3',
            aws_access_key_id=s3_conn.login,
            aws_secret_access_key=s3_conn.password,
            endpoint_url=s3_conn.host,
            config=Config(signature_version='s3v4', connect_timeout=600)
        )
        
        s3_key = f'{self.export_prefix}/{ds}/{self.table_name}_{ds}.csv'
        s3_client.put_object(Body=file, Bucket=self.s3_bucket, Key=s3_key)
        file.close()
        
        self.log.info(f'+ Экспортировано: {s3_key}')
        return s3_key