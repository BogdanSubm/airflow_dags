from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Функция для вывода "Hello World"
def print_hello():
    print("Hello World")

# Параметры DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 8),  # Начало: 08.03.2026
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создание DAG
with DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    description='FirstDag',
    schedule_interval='3 8 * * *',  # Запуск каждый день в 08:03
    catchup=True,
    tags=['example'],
) as dag:

    # Задача в начале — EmptyOperator
    start_task = EmptyOperator(
        task_id='start_task'
    )

    # Задача в середине — вывод Hello World
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    # Задача в конце — EmptyOperator
    end_task = EmptyOperator(
        task_id='end_task'
    )

    # Определение порядка выполнения задач
    start_task >> hello_task >> end_task