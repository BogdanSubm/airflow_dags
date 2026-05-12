"""
Кастомные операторы для динамической генерации таблиц.
"""

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from typing import Dict, Optional
import psycopg2 as pg
from psycopg2.extras import RealDictCursor
from io import BytesIO
import csv
import codecs


class DynamicTableOperator(BaseOperator):
    """
    Оператор для создания и наполнения таблиц.
    
    """
    
    template_fields = ('table_ddl', 'table_dml')
    ui_color = '#3498db'
    
    @apply_defaults
    def __init__(
        self,
        table_name: str,
        table_ddl: str,
        table_dml: str,
        pg_conn_id: str = 'conn_pg',
        pg_db: str = 'etl',
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.table_ddl = table_ddl
        self.table_dml = table_dml
        self.pg_conn_id = pg_conn_id
        self.pg_db = pg_db
    
    def execute(self, context):
        """Выполняет DDL и DML для таблицы."""
        self.log.info(f'{"="*50}')
        self.log.info(f'Обработка таблицы: {self.table_name}')
        
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
                
                # 1. Создание таблицы
                self.log.info('1/2 Создание таблицы (DDL)')
                cur.execute(self.table_ddl)
                conn.commit()
                self.log.info(f'+ Таблица {self.table_name} готова')
                
                # 2. Наполнение данными
                self.log.info('2/2 Наполнение данными (DML)')
                cur.execute(self.table_dml)
                row_count = cur.rowcount
                conn.commit()
                self.log.info(f'+ Обработано строк: {row_count}')
                
                # 3. Проверка результата
                cur.execute(f"SELECT COUNT(*) as cnt FROM {self.table_name}")
                total = cur.fetchone()['cnt']
                self.log.info(f'+ Всего строк в таблице: {total}')
        
        self.log.info(f'+ Таблица {self.table_name} успешно обработана')
        self.log.info(f'{"="*50}')
        
        return f'{self.table_name}: {row_count} rows'


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
        
        # 1. Получаем данные из PG
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
            
            # Пробуем load_date, если нет - created_at
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