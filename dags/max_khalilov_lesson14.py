from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor 
from airflow.sensors.time_delta import TimeDeltaSensor 
from airflow.hooks.base import BaseHook

from operators.operator_combine_date_max_khalilov import CustomCombineDataOperator
from operators.upload_data_operator_max_khalilov import CustomUploadDataOperator
from sensors.les_15_sensor_max_khalilov import SqlSensor

DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 4, 14)
}

with DAG(
    dag_id='max_khalilov_lesson14',
    tags=['max_khalilov', '7'],
    default_args=DEFAULT_ARGS,
    schedule='@daily',
    max_active_runs=1,
    max_active_tasks=1
) as dag:
    
    start_dag = EmptyOperator(task_id='start_dag')
    end_dagg = EmptyOperator(task_id='end_dag')

    wait_3_msk = TimeDeltaSensor(
        task_id='wait_3_msk', 
        delta=timedelta(hours=3), # параметр, сколько времени ждать. Обязательное поле
        mode='reschedule', # параметр, как будет работать датчик. Обязательное поле
        poke_interval=300, # параметр, как часто будет проверяться датчик. Обязательное поле
    )

    dag_sensor = ExternalTaskSensor(
        task_id='dag_sensor', # Имя задачи
        external_dag_id='max_khalilov_practic13', # Имя DAG'а, который будет проверяться
        execution_delta=timedelta(minutes=0), # ОЧЕНЬ ВАЖНЫЙ ПАРАМЕТР!!!!!!!!! Сколько времени ждать завершения DAG'а. Если это будущий DAG, тогда минус, если прошлый, тогда плюс, если они одинаковые, тогда 0.
        mode='reschedule', # Как будет работать датчик. Можно сказать обязательный параметр, потому что, если таска почему-то не выполняется и тд, она какбудто падает освобождая слот для другой таски, не занимая место.
        poke_interval=300, # Как часто будет проверяться датчик
    )

    sql_sensor = SqlSensor(
        task_id='max_khalilov_sql_sensor',
        sql="""
            SELECT COUNT(1)
                FROM maks_khalilov
            WHERE created_at >= '{{ ds }}'::timestamp
                AND created_at < '{{ ds }}'::timestamp + INTERVAL '1 days';
        """, # sql запрос, который будет проверяться. Фильтр where прописывается, чтобы проверять данные за последние 24 часа. И так как мы укази в template_fields ('sql',) то можно использовать {{ ds }}
        mode='reschedule', # Как будет работать датчик. Можно сказать обязательный параметр, потому что, если таска почему-то не выполняется и тд, она какбудто падает освобождая слот для другой таски, не занимая место.
        poke_interval=300, # Как часто будет проверяться датчик 
    )

    combine_data = CustomCombineDataOperator(
        task_id='combine_data',
    )

    upload_data = CustomUploadDataOperator(
        task_id='upload_data',
    )

    # Перенос тасков на другую строку можно сделать при помощи символа \. Т е данный вариант можно написать в одну строку
    start_dag >> wait_3_msk >> dag_sensor >> sql_sensor >>\
        combine_data >> upload_data >> end_dagg




