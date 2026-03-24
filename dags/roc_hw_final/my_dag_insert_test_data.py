from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


def load_test_data_to_pg():
    test_data = [
        (i, f'Category_{i % 5}', i * 10.5)
        for i in range(1, 51)
    ]

    pg_hook = PostgresHook(postgres_conn_id='conn_pg')

    pg_hook.insert_rows(
        table='roc_test_raw_data',
        rows=test_data,
        target_fields=['id', 'category', 'amount'],
        commit_every=1000
    )


DEFAULT_ARGS = {
    'owner': 'roc',
    'retries': 2,
    'retries_delay': 600,
    'start_date': datetime(2026, 2, 16)
}

test_data = [
    (i, f"Category_{i % 5}", round(i * 10.5, 2))
    for i in range(1, 51)
]

with DAG(
    dag_id='insert_test_datas',
    schedule=None,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    tags=['roc'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    create_raw_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='conn_pg',
        sql="create table if not exists roc_test_raw_data (id bigint, category text, amount int)",
        parameters=(test_data,)
    )

    insert_test_data = PythonOperator(
        task_id='insert_test_data',
        python_callable=load_test_data_to_pg
    )

    start >> create_raw_table >> insert_test_data >> end
