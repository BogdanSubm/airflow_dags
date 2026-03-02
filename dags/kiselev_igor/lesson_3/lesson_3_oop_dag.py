from airflow.models.dag import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
 
from datetime import datetime

from lesson_3.src.utils import python_test_func, python_test_2_func

with DAG(
    dag_id="lesson_3_oop",
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

    python_test_2 = PythonOperator(
        task_id = 'python_test_2',
        python_callable = python_test_2_func
    )

    start_dag >> python_test >> python_test_2 >> end_dag