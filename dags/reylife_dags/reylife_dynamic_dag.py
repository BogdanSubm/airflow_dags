from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

config = [
    {
        'table_name': 'reylife_agg_by_user',
        'table_ddl': """
            CREATE TABLE IF NOT EXISTS reylife_agg_by_user (
                lti_user_id TEXT,
                total_attempts INT,
                correct_answers INT
            )
        """,
        'table_dml': """
            DELETE FROM reylife_agg_by_user;
            INSERT INTO reylife_agg_by_user
            SELECT
                lti_user_id,
                COUNT(*) as total_attempts,
                COUNT(*) FILTER (WHERE is_correct = true) as correct_answers
            FROM reylife_raw
            GROUP BY lti_user_id
        """,
        'need_to_export': True
    },
    {
        'table_name': 'reylife_agg_by_day',
        'table_ddl': """
            CREATE TABLE IF NOT EXISTS reylife_agg_by_day (
                day DATE,
                total_attempts INT
            )
        """,
        'table_dml': """
            DELETE FROM reylife_agg_by_day;
            INSERT INTO reylife_agg_by_day
            SELECT
                created_at::date as day,
                COUNT(*) as total_attempts
            FROM reylife_raw
            GROUP BY created_at::date
        """,
        'need_to_export': False
    }
]

DEFAULT_ARGS = {
    "owner": "reylife",
    "start_date": datetime(2026, 6, 12),
    "retries": 2
}

with DAG(
    dag_id="reylife_dynamic_dag",
    schedule="@daily",
    tags=["reylife", "@TvoiRaiii"],
    default_args=DEFAULT_ARGS,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    for table in config:
        dummy = EmptyOperator(task_id=f"process_{table['table_name']}")
        start >> dummy >> end