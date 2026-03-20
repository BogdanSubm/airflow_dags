from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from rocknmove_hw.plugins.operators import TablesResetOperator

from rocknmove_hw.plugins import sqls


DEFAULT_ARGS = {
    'owner': 'rocknmove',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2026, 2, 16)
}

with DAG(dag_id='rocknmove_all_tables_reset',
         schedule='@once',
         default_args=DEFAULT_ARGS,
         tags=['rocknmove'],
         ) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    reset_named_tables = TablesResetOperator(
        task_id='reset_named_tables',
        conn_id='conn_pg',
        tables_to_reset=('rocknmove_raw_data', 'rocknmove_users', 'rocknmove_data_agg1'),
        sql_creates_dict=sqls.sql_creates_dict
    )

    dag_start >> reset_named_tables >> dag_end
