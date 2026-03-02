from airflow.models.dag import DAG 
from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.hooks.base import BaseHook 

def test_func(**context):
    test_conn = BaseHook.get_connection('test_conn')

    print(test_conn.host)
    print(test_conn.port)
    print(test_conn.login)

    pw = test_conn.password

    for el in pw:
        print(el)

with DAG(
    dag_id="lesson_4",
    schedule='@once',
    start_date=datetime(2025, 9, 1)
) as dag:
    
    start_dag = EmptyOperator(
        task_id = 'start_dag',
    )
    end_dag = EmptyOperator(
        task_id = 'end_dag',
    )

    test = PythonOperator(
        task_id='test',
        python_callable=test_func, 
    )

    start_dag >> test >> end_dag 
