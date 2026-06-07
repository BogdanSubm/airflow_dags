from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from enjout.src.utils import (
    aggregate_data,
    export_aggregated_to_s3,
    load_raw_data,
)

DEFAULT_ARGS = {
    'owner': 'enjout',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2025, 1, 1),
}


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


with DAG(
    dag_id='enjout_weekly_jinja',
    tags=['enjout', 'weekly', 'jinja', 'api', 'postgres', 's3'],
    description='Еженедельная выгрузка с Jinja-шаблонами',
    schedule='0 7 * * 1',
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    user_defined_macros={
        'previous_week_start': WeekTemplates.previous_week_start,
        'previous_week_end': WeekTemplates.previous_week_end,
    },
    render_template_as_native_obj=True,
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    jinja_kwargs = {
        'week_start': '{{ previous_week_start(ds) }}',
        'week_end': '{{ previous_week_end(ds) }}',
        'execution_date': '{{ ds }}',
    }

    load_raw_data_task = PythonOperator(
        task_id='load_raw_data',
        python_callable=load_raw_data,
        op_kwargs=jinja_kwargs,
        execution_timeout=timedelta(hours=1),
    )

    aggregate_data_task = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
        op_kwargs=jinja_kwargs,
        execution_timeout=timedelta(hours=1),
    )

    export_aggregated_to_s3_task = PythonOperator(
        task_id='export_aggregated_to_s3',
        python_callable=export_aggregated_to_s3,
        op_kwargs=jinja_kwargs,
        execution_timeout=timedelta(hours=1),
    )

    (
        dag_start
        >> load_raw_data_task
        >> aggregate_data_task
        >> export_aggregated_to_s3_task
        >> dag_end
    )
