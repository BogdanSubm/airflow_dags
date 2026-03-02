from airflow.models.dag import DAG 
from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

def test_func(ds: str, **context):
    print(f'ds: {ds}')
    print('------------')
    print(context)

with DAG(
    dag_id="first_dag",
    schedule='@daily',
    start_date=datetime(2025, 12, 1)
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
        op_kwargs={
            'ds': '{{ ds }}'
        },
    )

    start_dag >> test >> end_dag 