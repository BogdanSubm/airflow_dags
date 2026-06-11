from airflow.models.dag import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

def test_func(ds: str, **context ):
    print(f'ds: {ds}')
    print('_________')
    print(context)        



with DAG (
    dag_id ='tolstyakoff_first_dag',
    schedule = '@daily',
    start_date = datetime(2026, 6, 11), 
) as dag:
   
    start_dag = EmptyOperator(
        task_id='start',
    )
    
    end_dag = EmptyOperator(
        task_id='end',
    )
    
    test = PythonOperator (
        task_id='test',
        python_callable = test_func,
        op_kwargs = {
            'ds': '{{ ds }}'
        },
    )

    start_dag >> test >> end_dag