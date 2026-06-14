from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from syomin.syomin_final_project.config.syomin_config import config
from syomin.syomin_final_project.utils import export_table_to_s3

DEFAULT_ARGS = {
    'owner': 'syomin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='syomin_dynamic_aggregates',
    tags=['final', 'syomin'],
    description='Динамическое создание агрегатов (ежедневно)',
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')
    
    first_tasks = []
    last_tasks = []
        
    for agg in config:
        table = agg['table_name']
        create = PostgresOperator(
            task_id=f'create_{table}',
            postgres_conn_id='conn_pg',
            sql=agg['table_ddl'],
        )
        load = PostgresOperator(
            task_id=f'load_{table}',
            postgres_conn_id='conn_pg',
            sql=agg['table_dml'],
        )
        create >> load
        first_tasks.append(create)

        if agg['need_to_export']:
            export = PythonOperator(
                task_id=f'export_{table}',
                python_callable=export_table_to_s3,
                op_kwargs={
                    'table_name': table,
                    'bucket_name': 'airflow-exports',
                    's3_conn_id': 'conn_s3'
                },
            )
            load >> export
            last_tasks.append(export)
        else:
            last_tasks.append(load)
    if first_tasks:
        dag_start >> first_tasks
    if last_tasks:
        last_tasks >> dag_end