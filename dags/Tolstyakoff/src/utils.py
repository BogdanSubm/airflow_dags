from datetime import datetime
from airflow.hooks.base import BaseHook

def python_test_func(animal:str, **context)-> None:
    print(animal)
    print(context['ds'])
    print(datetime.now())


def lesson3_test1_func(**context) -> None:
    print(context['dag'].dag_id)

def lesson3_test2_func(animal: str, **context) -> None:
    print(context['ti'].task_id)

def python_db_func ( **context)-> None:
    test_conn = BaseHook.get_connection('Tolstyakoff_conn')
    print(test_conn.host)
    print(test_conn.port)
    print(test_conn.login)
    print(test_conn.password)
    
    
    
    
