from datetime import datetime
from airflow import DAG 
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator 
from airflow.hooks.base import BaseHook 

from src.utils import load_from_api, API_URL

DEFAULT_ARGS = {
    "owner": "tkachev",
    "retries": 2, 
    "retry_delay": 600,
    "start_date": datetime(2026, 5, 10)
}

with DAG( 
    dag_id="tkachev_load_from_api_to_pg",
    tags=["tkachev"],
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag: 
    
    dag_start = EmptyOperator(taks_id="dag_start")
    dag_end = EmptyOperator(task_id="dag_end")

    load_from_api = PythonOperator(
        task_id="load_from_api",
        pythom_callable=load_from_api
    )

    dag_start >> load_from_api >> dag_end
