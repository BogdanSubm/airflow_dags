# TRR_lesson4_dag.py 
# ОТрабатываем подключения
from airflow.models.dag         import DAG
from airflow.operators.python   import PythonOperator
from airflow.operators.empty    import EmptyOperator

# Есть в утилите
# from airflow.hooks.base import BaseHook

from datetime import datetime

import sys
import os

# Добавляем текущую папку (Tolstyakoff) в путь
# Теперь можно импортировать из src
sys.path.append(os.path.dirname(__file__))
from src.utils import python_db_func


with DAG (
    dag_id ='tolstyakoff_lesson4_dag',
    schedule = '*/10 * * * *',
    start_date = datetime(2026, 6, 11), 
    catchup=False,  # 👈 чтобы не гонял впустую
) as dag:
    
    start_dag = EmptyOperator(task_id='start') #для маркировки начала и конца дага
    end_dag = EmptyOperator(task_id='end')

    DB_test = PythonOperator (
        task_id='DB_test',
        python_callable = python_db_func,
    )

    start_dag >> DB_test >> end_dag
