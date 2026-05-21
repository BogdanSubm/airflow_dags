from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from bubble.utils.soldatowiw_tasks import load_from_api, aggregate


default_args = {
    'owner': 'soldatowiw',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2026, 5, 1),
}

with DAG(
    'soldatowiw_load_from_api',
    default_args=default_args,
    schedule='@daily',
    max_active_runs=1,
    max_active_tasks=1,
    catchup=True,
) as dag:
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    load_task = PythonOperator(
        task_id='load_from_api',
        python_callable=load_from_api,
    )

    agg_task = PythonOperator(
        task_id='aggregate',
        python_callable=aggregate,
    )

dag_start >> load_task >> agg_task >> dag_end
