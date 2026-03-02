from datetime import datetime

def python_test_func(**context) -> None:
    print(context['dag'].dag_id)

def python_test_2_func(**context) -> None:
    print(context['ti'].task_id)