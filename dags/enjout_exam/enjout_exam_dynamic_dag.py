from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from enjout.src.utils import load_raw_data
from enjout_exam.config import DAG_CONFIG, TABLE_CONFIGS
from enjout_exam.operators.postgres_operator import PostgresOperator
from enjout_exam.utils import (
    build_idempotency_sql,
    build_insert_sql,
    check_data_quality,
    export_table_to_s3,
)


class WeekTemplates:
    @staticmethod
    def previous_week_start(date: str) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        week_start = logical_dt - timedelta(days=logical_dt.weekday() + 7)
        return week_start.strftime('%Y-%m-%d')

    @staticmethod
    def previous_week_end(date: str) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        week_start = logical_dt - timedelta(days=logical_dt.weekday() + 7)
        week_end = week_start + timedelta(days=6)
        return week_end.strftime('%Y-%m-%d')


jinja_kwargs = {
    'week_start': '{{ previous_week_start(ds) }}',
    'week_end': '{{ previous_week_end(ds) }}',
    'execution_date': '{{ ds }}',
}

with DAG(
    dag_id=DAG_CONFIG['dag_id'],
    schedule=DAG_CONFIG['schedule'],
    start_date=DAG_CONFIG['start_date'],
    catchup=DAG_CONFIG['catchup'],
    max_active_runs=DAG_CONFIG['max_active_runs'],
    max_active_tasks=DAG_CONFIG['max_active_tasks'],
    tags=DAG_CONFIG['tags'],
    description=DAG_CONFIG['description'],
    default_args={
        'owner': 'enjout',
        'retries': 2,
        'retry_delay': 600,
    },
    user_defined_macros={
        'previous_week_start': WeekTemplates.previous_week_start,
        'previous_week_end': WeekTemplates.previous_week_end,
    },
    render_template_as_native_obj=True,
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    load_raw_data_task = PythonOperator(
        task_id='load_raw_data',
        python_callable=load_raw_data,
        op_kwargs=jinja_kwargs,
        execution_timeout=timedelta(hours=1),
    )

    group_end_tasks = []

    for table_config in TABLE_CONFIGS:
        table_name = table_config['table_name']
        group_id = f'group_{table_name.replace("enjout_exam_", "")}'

        with TaskGroup(group_id=group_id, tooltip=f'Агрегат {table_name}') as table_group:
            create_table_task = PostgresOperator(
                task_id='create_table',
                sql=table_config['table_ddl'],
            )

            fill_table_task = PostgresOperator(
                task_id='fill_table',
                sql=[
                    build_idempotency_sql(table_name),
                    build_insert_sql(table_name, table_config['table_dml']),
                ],
            )

            check_quality_task = PythonOperator(
                task_id='check_quality',
                python_callable=check_data_quality,
                op_kwargs={
                    'table_name': table_name,
                    'week_start': '{{ previous_week_start(ds) }}',
                    'week_end': '{{ previous_week_end(ds) }}',
                    'min_rows': table_config.get('min_rows', 0),
                    'execution_date': '{{ ds }}',
                },
            )

            create_table_task >> fill_table_task >> check_quality_task

            if table_config.get('need_to_export', False):
                export_task = PythonOperator(
                    task_id='export_to_s3',
                    python_callable=export_table_to_s3,
                    op_kwargs={
                        'table_name': table_name,
                        'week_start': '{{ previous_week_start(ds) }}',
                        'week_end': '{{ previous_week_end(ds) }}',
                        'execution_date': '{{ ds }}',
                    },
                )
                check_quality_task >> export_task
                group_end_tasks.append(export_task)
            else:
                group_end_tasks.append(check_quality_task)

        dag_start >> load_raw_data_task >> table_group

    for end_task in group_end_tasks:
        end_task >> dag_end
