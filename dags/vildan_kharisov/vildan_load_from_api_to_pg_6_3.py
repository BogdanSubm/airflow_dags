from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from vildan_kharisov.vildan_api_to_pg_operator import APIToPgOperator
from vildan_kharisov.vildan_branch_operator import CustomBranchOperator
from vildan_kharisov.vildan_pg_operator import PostgresOperator

DEFAULT_ARGS = {
    'owner': 'vildan_kharisov',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 3, 17),
}

sql_query = """
    INSERT INTO vildan_agg_table
    SELECT lti_user_id,
           attempt_type,
           COUNT(1),
           COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_failed_count,
           '{{ds}}'::timestamp
      FROM vildan_kharisov_table
     WHERE created_at >= '{{ds}}'::timestamp
           AND created_at < '{{ds}}'::timestamp + INTERVAL '1 days'
      GROUP BY lti_user_id, attempt_type;
"""


with DAG(
    dag_id="vildan_load_from_api_to_pg_with_operator_and_branch",
    tags=['6', 'vildan'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    branch = CustomBranchOperator(
        task_id='branch'
    )

    load_from_api = APIToPgOperator(
        task_id='load_from_api',
        date_from='{{ ds }}',
        date_to='{{ macros.ds_add(ds, 1) }}',
    )
    sql_to_pg = PostgresOperator(
        task_id='sql_to_pg',
        sql_query = sql_query,
    )

    dag_start >> branch >> load_from_api >> sql_to_pg >> dag_end
