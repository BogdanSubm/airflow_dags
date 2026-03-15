from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    'owner': 'rocknmove',
    'retries': 2,
    'retries_delay': 600,
    'start_date': datetime(2026, 2, 16)
}

with DAG(dag_id='rocknmove_lib_show',
         schedule='@once',
         start_date=datetime.today(),
         default_args=DEFAULT_ARGS,
         tags=['rocknmove']) as dag:

    start_dag = EmptyOperator(task_id='start_dag')
    end_dag = EmptyOperator(task_id='end_dag')

    lib_test = BashOperator(
        task_id='lib_test',
        bash_command='pip freeze'
    )

    start_dag >> lib_test >> end_dag
