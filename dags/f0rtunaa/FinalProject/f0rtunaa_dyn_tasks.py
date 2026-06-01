from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


import sys
from pathlib import Path
dags_path = Path(__file__).parent
sys.path.append(str(dags_path))

from f0rtunaa_Conf import config
from f0rtunaa_utils import upload_to_s3
from OP.f0rtunaa_Agg_op import AggTableOperator


DEFAULT_ARGS = {
    'owner': 'f0rtunaa',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2026, 6, 1),
}

with DAG(
    dag_id='f0rtunaaFinalProject',
    tags=['5', 'f0rtunaa'],
    schedule='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end   = EmptyOperator(task_id='dag_end')

    for table_cfg in config:
        t_name = table_cfg['table_name']
        need_export = table_cfg.get('need_to_export', False)

        # Таск 1: создать таблицу + наполнить данными
        agg_task = AggTableOperator(
            task_id=f'agg_{t_name}',
            table_name=t_name,
            table_ddl=table_cfg['table_ddl'],
            table_dml=table_cfg['table_dml'],
       )

        if need_export:
            # Таск 2 : выгрузить в S3
            export_task = PythonOperator(
                task_id=f'export_{t_name}',
                python_callable=upload_to_s3,
                op_kwargs={
                    'table_name': t_name,
                },
            )
            dag_start >> agg_task >> export_task >> dag_end
        else:
            dag_start >> agg_task >> dag_end