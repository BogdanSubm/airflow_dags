from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from operators.extract_operator import ExtractDataOperator
from operators.export_operator import ExportCsvOperator
from operators.branch_operator import CustomBranchOperator
from operators.postgres_operator import CustomPostgresOperator


# дни месяца когда запускаем расчёты
ACTIVE_DAYS = [1, 2, 13]

DEFAULT_ARGS = {
    "owner": "reylife",
    "start_date": datetime(2026, 6, 10),
    "retries": 2
}

SQL_CREATE_RAW = """
    CREATE TABLE IF NOT EXISTS reylife_raw (
        lti_user_id     TEXT,
        passback_params TEXT,
        is_correct      BOOLEAN,
        attempt_type    TEXT,
        created_at      TIMESTAMP,
        PRIMARY KEY (lti_user_id, created_at)
    )
"""

SQL_CREATE_AGG = """
    CREATE TABLE IF NOT EXISTS reylife_agg (
        total_rows      INT,
        unique_users    INT,
        correct_answers INT
    )
"""

SQL_AGGREGATE = """
    DELETE FROM reylife_agg;

    INSERT INTO reylife_agg
    SELECT
        COUNT(*)                    AS total_rows,
        COUNT(DISTINCT lti_user_id) AS unique_users,
        COUNT(*) FILTER (
            WHERE is_correct = true
        )                           AS correct_answers
    FROM reylife_raw;
"""


with DAG(
    dag_id="reylife_dag",
    schedule="@daily",
    tags=["reylife", "@TvoiRaiii"],
    default_args=DEFAULT_ARGS,
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start")

    branch = CustomBranchOperator(
        task_id="branch",
        active_days=ACTIVE_DAYS,
        active_task_id="create_tables",
        skip_task_id="export_csv"
    )

    create_tables = CustomPostgresOperator(
        task_id="create_tables",
        sql=[SQL_CREATE_RAW, SQL_CREATE_AGG]
    )

    extract = ExtractDataOperator(
        task_id="extract_data",
        start_date="{{ macros.ds_add(ds, -7) }}",
        end_date="{{ ds }}"
    )

    aggregate = CustomPostgresOperator(
        task_id="aggregate_data",
        sql=SQL_AGGREGATE
    )

    export = ExportCsvOperator(
        task_id="export_csv",
        trigger_rule="none_failed_min_one_success"
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    start >> branch >> [create_tables, export]
    create_tables >> extract >> aggregate >> export
    export >> end