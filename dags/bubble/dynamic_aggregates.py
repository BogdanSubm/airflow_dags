from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from bubble.postgres_operator import PostgresOperator
from bubble.utils.soldatowiw_tasks import export_to_s3


config = [
    {
        'table_name': 'soldatowiw_dyn_by_type',
        'table_ddl': (
            'CREATE TABLE IF NOT EXISTS soldatowiw_dyn_by_type ('
            'attempt_type TEXT, '
            'attempts BIGINT, '
            'correct BIGINT, '
            'ds DATE'
            ')'
        ),
        'table_dml': (
            'SELECT attempt_type, '
            'COUNT(*) AS attempts, '
            'SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) AS correct, '
            "'{{ ds }}'::date AS ds "
            'FROM soldatowiw_raw_table '
            "WHERE ds = '{{ ds }}' "
            'GROUP BY attempt_type'
        ),
        'need_to_export': True,
    },
    {
        'table_name': 'soldatowiw_dyn_by_user',
        'table_ddl': (
            'CREATE TABLE IF NOT EXISTS soldatowiw_dyn_by_user ('
            'lti_user_id TEXT, '
            'attempts BIGINT, '
            'ds DATE'
            ')'
        ),
        'table_dml': (
            'SELECT lti_user_id, '
            'COUNT(*) AS attempts, '
            "'{{ ds }}'::date AS ds "
            'FROM soldatowiw_raw_table '
            "WHERE ds = '{{ ds }}' "
            'GROUP BY lti_user_id'
        ),
        'need_to_export': False,
    },
]


default_args = {
    'owner': 'soldatowiw',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2026, 6, 1),
}

with DAG(
    'soldatowiw_dynamic_aggregates',
    default_args=default_args,
    schedule='@daily',
    max_active_runs=1,
    max_active_tasks=1,
    catchup=True,
) as dag:
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    for cfg in config:
        name = cfg['table_name']

        create_task = PostgresOperator(
            task_id='create_' + name,
            sql=cfg['table_ddl'],
            autocommit=True,
        )

        delete_sql = "DELETE FROM " + name + " WHERE ds = '{{ ds }}'"
        insert_sql = "INSERT INTO " + name + "\n" + cfg['table_dml']
        fill_task = PostgresOperator(
            task_id='fill_' + name,
            sql=[delete_sql, insert_sql],
        )

        dag_start >> create_task >> fill_task

        if cfg['need_to_export']:
            export_task = PythonOperator(
                task_id='export_' + name,
                python_callable=export_to_s3,
                op_kwargs={'table_name': name, 'ds': '{{ ds }}'},
            )
            fill_task >> export_task >> dag_end
        else:
            fill_task >> dag_end
