from typing import Any

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, SkipMixin


class CustomBranchOperator(BaseOperator, SkipMixin):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context: Any):
        dt = pendulum.parse(context['ds'])

        tasks_to_execute = []

        if dt.weekday() in [0, 4, 6]:
            tasks_to_execute.append('load_from_api')

        valid_task_ids = set(context["dag"].task_ids)

        invalid_task_ids = set(tasks_to_execute) - valid_task_ids

        if invalid_task_ids:
            raise AirflowException(
                f"Branch callable must return valid task_ids. "
                f"Invalid tasks found: {invalid_task_ids}"
            )

        self.skip_all_except(context['ti'], set(tasks_to_execute))