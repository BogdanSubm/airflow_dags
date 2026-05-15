from datetime import datetime
from airflow import DAG 
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator 

from tkachev.src.combine_data import combine_data
from tkachev.src.upload_data import upload_data


DEFAULT_ARGS = {
    "owner": "tkachev",
    "retries": 2, 
    "retry_delay": 600,
    "start_date": datetime(2026, 5, 14)
}

with DAG( 
    dag_id="tkachev_combine_api_data",
    tags=["tkachev"],
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag: 
    
    dag_start = EmptyOperator(task_id="dag_start")
    dag_end = EmptyOperator(task_id="dag_end")

    combine_data = PythonOperator(
        task_id="combine_data",
        python_callable="combine_data"
    )

    upload_data = PythonOperator(
        task_id="upload_data",
        python_callable=upload_data
    )

    dag_start >> combine_data >> upload_data >> dag_end