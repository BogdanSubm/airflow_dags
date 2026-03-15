from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import rocknmove_hw.plugins.utils_lesson8 as myutils


DEFAULT_ARGS = {
    'owner': 'rocknmove',
    'retries': 2,
    'retries_delay': 600,
    'start_date': datetime(2026, 2, 16)
}


with DAG(dag_id='rocknmove_delete_named_tables',
         schedule='@once',
         start_date=datetime.today()) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    delete_named_tables = PythonOperator(
        task_id='delete_named_tables',
        python_callable=myutils.delete_named_tables
    )
