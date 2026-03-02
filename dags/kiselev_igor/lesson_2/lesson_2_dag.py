from airflow.models.dag import DAG 
from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from lesson_2.src.utils import python_test_func

with DAG(
    dag_id="lesson_2",
    schedule='@once',
    start_date=datetime(2024, 9, 1)
) as dag:

    start_dag = EmptyOperator(
        task_id = 'start_dag',
    )
    end_dag = EmptyOperator(
        task_id = 'end_dag',
    )

    python_test = PythonOperator(
        task_id = 'python_test',
        python_callable = python_test_func,
        op_kwargs = {
            "animal": "dog"
        }
    )

    bash_test = BashOperator(
        task_id = 'bash_test',
        bash_command = 'echo "Hello, world!"',
    )

    bash_test_2 = BashOperator(
        task_id = 'bash_test_2', 
        bash_command = 'pip freeze',
    )

    start_dag >> python_test >> end_dag

    start_dag >> bash_test >> bash_test_2 >> end_dag