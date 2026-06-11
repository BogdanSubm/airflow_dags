from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from sensors.multi_sql_sensor_pntra import MultiSQLSensor




DEFAULT_ARGS = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 2, 16),
}



with DAG(
    dag_id="multi_sql_sensor_dag_pntra",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
   
    tags=["custom_sensor", "sql"],
) as dag:
    start = EmptyOperator(task_id="start")

    wait_for_tables = MultiSQLSensor(
        task_id="wait_for_tables",
        conn_id="conn_pg",
        sql_checks=[
            """
            SELECT COUNT(*)
            FROM admin_table_pntra
            WHERE created_at::date = '{{ ds }}'
            """,
       
     
        ],
        poke_interval=300,
        timeout=60 * 30,
        mode="reschedule",
    )
    finish = EmptyOperator(task_id="finish")
    start >> wait_for_tables >> finish