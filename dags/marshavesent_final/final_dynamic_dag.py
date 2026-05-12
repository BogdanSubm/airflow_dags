"""
Динамический DAG для генерации таблиц на основе конфигурации.

Принцип работы:
1. Читает TABLE_CONFIGS из dynamic_table_config.py
2. Для каждой таблицы создает Task (и опционально Task экспорта)
3. Таблицы обрабатываются параллельно (max_active_tasks=3)

Идемпотентность:
- CREATE TABLE IF NOT EXISTS (не пересоздает таблицы)
- INSERT ON CONFLICT DO UPDATE (обновляет записи)
- Экспорт перезаписывает файл за дату

Как добавить таблицу:
- Добавить словарь в TABLE_CONFIGS в dynamic_table_config.py
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
import pendulum
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from marshavesent_final.dynamic_table_config import TABLE_CONFIGS, DAG_CONFIG, CONNECTION_CONFIG
from marshavesent_final.dynamic_table_operators import DynamicTableOperator, DynamicExportOperator

with DAG(
    dag_id='marshavesent_dynamic_tables',
    schedule_interval=DAG_CONFIG['schedule_interval'],
    start_date=DAG_CONFIG['start_date'],
    catchup=DAG_CONFIG['catchup'],
    max_active_runs=DAG_CONFIG['max_active_runs'],
    max_active_tasks=DAG_CONFIG['max_active_tasks'],
    tags=DAG_CONFIG['tags'],
    description=DAG_CONFIG['description'],
    default_args={
        'owner': 'marshavesent',
        'retries': 2,
        'retry_delay': 300,
        'depends_on_past': False,
    },
    render_template_as_native_obj=True,
    user_defined_macros={
        #Jinja макросы для SQL
        'yesterday_ds': lambda execution_date: (
            pendulum.instance(execution_date).subtract(days=1).to_date_string()
        ),
        'tomorrow_ds': lambda execution_date: (
            pendulum.instance(execution_date).add(days=1).to_date_string()
        ),
    }
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')
    
    chain_end_tasks = []
    
    # Задачи для каждой таблицы
    for config in TABLE_CONFIGS:
        
        table_name = config['table_name']
        
        task_suffix = table_name.replace('marshavesent_', '')
        
        # Основной оператор: создание + наполнение
        manage_table = DynamicTableOperator(
            task_id=f'manage_{task_suffix}',
            table_name=table_name,
            table_ddl=config['table_ddl'],
            table_dml=config['table_dml'],
            pg_conn_id=CONNECTION_CONFIG['pg_conn_id'],
            pg_db=CONNECTION_CONFIG['pg_db']
        )
        
        # Если нужен экспорт 
        if config.get('need_to_export', False):
            export_table = DynamicExportOperator(
                task_id=f'export_{task_suffix}',
                table_name=table_name,
                export_prefix=config.get('export_prefix', task_suffix),
                pg_conn_id=CONNECTION_CONFIG['pg_conn_id'],
                pg_db=CONNECTION_CONFIG['pg_db'],
                s3_conn_id=CONNECTION_CONFIG['s3_conn_id'],
                s3_bucket=CONNECTION_CONFIG['s3_bucket']
            )
            
            manage_table >> export_table
            chain_end_tasks.append(export_table)
        else:
            chain_end_tasks.append(manage_table)
    
    dag_start >> chain_end_tasks >> dag_end