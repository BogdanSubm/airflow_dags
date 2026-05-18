"""
Динамический DAG с TaskGroup для каждой таблицы.

Каждая таблица получает свою TaskGroup с отдельными шагами:
create_table → insert_data → check_quality → export (опционально)

Использует Airflow Variables для конфигурации подключений.
Не использует sys.path.insert - импорт через стандартный путь.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import pendulum

from marshavesent_final.dynamic_table_config import (
    TABLE_CONFIGS, 
    DAG_CONFIG, 
    get_connection_config,
    QUALITY_THRESHOLDS
)
from marshavesent_final.dynamic_table_operators import (
    CreateTableOperator,
    InsertDataOperator,
    DataQualityOperator,
    DynamicExportOperator
)


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
        'retries': 1,
        'retry_delay': 300,
        'depends_on_past': False,
    },
    render_template_as_native_obj=True,
    user_defined_macros={
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
    
    conn_config = get_connection_config()
    
    all_group_end_tasks = []
    
    for config in TABLE_CONFIGS:
        
        table_name = config['table_name']
        group_id = table_name.replace('marshavesent_', 'group_')
        
        quality = QUALITY_THRESHOLDS.get(table_name, {
            'min_rows': conn_config['min_rows_threshold'],
            'max_null_pct': 10,
            'critical_columns': []
        })
        
        with TaskGroup(group_id=group_id, tooltip=f'Обработка {table_name}') as table_group:
            
            create_table = CreateTableOperator(
                task_id='create_table',
                table_name=table_name,
                table_ddl=config['table_ddl'],
                pg_conn_id=conn_config['pg_conn_id'],
                pg_db=conn_config['pg_db']
            )
            
            insert_data = InsertDataOperator(
                task_id='insert_data',
                table_name=table_name,
                table_dml=config['table_dml'],
                pg_conn_id=conn_config['pg_conn_id'],
                pg_db=conn_config['pg_db']
            )
            
            check_quality = DataQualityOperator(
                task_id='check_quality',
                table_name=table_name,
                min_rows=quality['min_rows'],
                max_null_pct=quality['max_null_pct'],
                critical_columns=quality['critical_columns'],
                pg_conn_id=conn_config['pg_conn_id'],
                pg_db=conn_config['pg_db']
            )
            
            create_table >> insert_data >> check_quality
            
            if config.get('need_to_export', False):
                export_table = DynamicExportOperator(
                    task_id='export_to_s3',
                    table_name=table_name,
                    export_prefix=config.get('export_prefix', table_name),
                    pg_conn_id=conn_config['pg_conn_id'],
                    pg_db=conn_config['pg_db'],
                    s3_conn_id=conn_config['s3_conn_id'],
                    s3_bucket=conn_config['s3_bucket']
                )
                
                check_quality >> export_table
                all_group_end_tasks.append(export_table)
            else:
                all_group_end_tasks.append(check_quality)
        
        dag_start >> table_group
    
    for end_task in all_group_end_tasks:
        end_task >> dag_end