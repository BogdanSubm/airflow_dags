from typing import Any # Тип данных для переменных

import pendulum # Библиотека для работы с датами
from airflow.exceptions import AirflowException # Исключение для Airflow
from airflow.models import BaseOperator, SkipMixin # Базовый класс для всех операторов и класс для пропуска задачи

class CustomBranchOperator(BaseOperator, SkipMixin):

    def __init__(self, **kwargs):
        super().__init__(**kwargs) # Вызываем конструктор родительского класса

def execute(self, context: Any):
    df = pendulum.parse(context['ds'])
    self.log.info(f"Execution date: {context['ds']}, Weekday: {df.weekday()}")
    tasks_to_execute = []
    if df.weekday() in [0, 4, 6]:
        tasks_to_execute.append('load_from_api')
        self.log.info("Task load_from_api will be executed")
    else:
        self.log.info("No tasks to execute, skipping load_from_api")
    valid_task_ids = set(context['dag'].task_ids)
    invalid_task_ids = set(tasks_to_execute) - valid_task_ids
    if invalid_task_ids:
        raise AirflowException(f"Branch callable must return valid task_ids. Invalid tasks_found: {invalid_task_ids}")
    self.log.info(f"Tasks to execute: {tasks_to_execute}")
    self.skip_all_except(context['ti'], set(tasks_to_execute))