from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from airflow.utils.task_group import TaskGroup

from config.agg_config import config
from operators.db_operator import create_table, load_table
from operators.s3_uploader import upload_to_s3

DEFAULT_ARGS = {
    'owner': 'spiridonov',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 12),
}

with DAG(
    dag_id='final_dag',
    tags=['final', 'spiridonov'],
    default_args=DEFAULT_ARGS,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    task_groups = []

    for cfg in config:
        table_name = cfg['table_name']

        with TaskGroup(group_id=f'{table_name}_pipeline') as tg:
            create = PythonOperator(
                task_id=f'create_{table_name}',
                python_callable=create_table,
                op_kwargs={
                    'table_name': table_name,
                    'ddl': cfg['table_ddl'],
                }
            )
            load = PythonOperator(
                task_id=f'load_{table_name}',
                python_callable=load_table,
                op_kwargs={
                    'table_name': table_name,
                    'dml': cfg['table_dml'],
                    'ds': '{{ ds }}'
                }
            )

            create >> load

            if cfg.get('need_to_export'):
                export = PythonOperator(
                    task_id=f'export_{table_name}_to_s3',
                    python_callable=upload_to_s3,
                    op_kwargs={
                        'table_name': table_name,
                        'ds': '{{ ds }}'
                    },
                    retries=3
                )
                load >> export

        task_groups.append(tg)

    start >> task_groups >> end