from typing import Any # Тип данных для переменных

import pendulum # Библиотека для работы с датами
from airflow.exceptions import AirflowException # Исключение для Airflow
from airflow.models import BaseOperator, SkipMixin # Базовый класс для всех операторов и класс для пропуска задачи

class CustomBranchOperator(BaseOperator, SkipMixin):

    def __init__(self, **kwargs):
        super().__init__(**kwargs) # Вызываем конструктор родительского класса

    def execute(self, context: Any): # Any - это тип данных для переменных, т е любой тип данных
        df = pendulum.parse(context['ds'])

        tasks_to_execute = []

        if df.weekday() in [0, 4, 6]:
            tasks_to_execute.append('load_from_api') # Добавляем задачу в список задач, которые нужно выполнить

        valid_task_ids = set(context['dag'].task_ids) # Получаем список всех задач в DAG. Конструкция context['dag'].task_ids - это список всех задач в DAG.

        invalid_task_ids = set(tasks_to_execute) - valid_task_ids # Получаем список задач, которые не входят в список задач, которые нужно выполнить

        if invalid_task_ids: # Если есть задачи, которые не входят в список задач, которые нужно выполнить, то вызываем исключение
            raise AirflowException( # Исключение для Airflow
                f"Branch callable must return valid task_ids. " # Сообщение об ошибке
                f"Invalid tasks_found: {invalid_task_ids}" # Сообщение об ошибке
            )

        self.skip_all_except(context['ti'], set(tasks_to_execute)) # Пропускаем все задачи, кроме тех, которые входят в список задач, которые нужно выполнить

