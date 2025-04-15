from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook

DEFAULT_ARGS = {}

class WeekTemplates:
    @staticmethod # Декоратор, который говорит, что метод не зависит от экземпляра класса, а только от класса.
    def current_week_start(date, type: str) -> str: # type: str - это тип возвращаемого значения. А -> обозначает, что возвращаемый тип - str.
        logical_dt = datetime.strptime(date, '%Y-%m-%d')

        current_week_start = logical_dt - timedelta(days=logical_dt.weekday()) # timedelta - разница между двумя датами. days - количество дней. weekday - номер дня недели. Здесь происходит вычисление первого дня недели.

        return current_week_start.strftime('%Y-%m-%d')

    @staticmethod
    def current_week_end(date):
        logical_dt = datetime.strptime(date, '%Y-%m-%d')

        current_week_end = logical_dt + timedelta(days=6 - logical_dt.weekday())

        return current_week_end.strftime('%Y-%m-%d')
    
def upload_data(week_start: str, week_end: str, **context):
    pass

def combine_data(week_start: str, week_end: str, **context):
    pass

with DAG(
    dag_id='max_khalilov_custom',
    tags=['max_khalilov', '7'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    user_defined_macros={
        'current_week_start': WeekTemplates.current_week_start,
        'current_week_end': WeekTemplates.current_week_end
    },
    render_template_as_native_obj=True
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')

    combine_data = PythonOperator(
        task_id='combine_data', # Имя задачи
        python_calleble=combine_data, # Функция, которая будет вызвана
        params= {
            type: 'name'
        },
        op_kwargs={
            'week_start': '{{ current_week_start(ds, params.type) }}', # Параметры функции. Фигурные скобки это и есть template. Но таких в базе мы не встретим, они кастомные.
            'week_end': '{{ current_week_end(ds)}}',
        }
    )

    upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
        op_kwargs={
            'week_start': '{{ current_week_start(ds) }}',
            'week_end': '{{ current_week_end(ds) }}',
        }
    )

    dag_start >> combine_data >> upload_data >> dag_end

    