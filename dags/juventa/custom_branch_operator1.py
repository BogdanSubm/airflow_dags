from typing import Any, Sequence

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, SkipMixin

class JCustomBranchOperatorNew(BaseOperator, SkipMixin):  #SkipMixin - помогает скипать таски

    template_fields = ('weekdays', 'task_id_exclude')

    def __init__(
            self,
            weekdays: Sequence[int],
            task_id_exclude: str,
            **kwargs):
        super().__init__(**kwargs)
        self.weekdays = list(weekdays)
        self.task_id_exclude = task_id_exclude

    def execute(self, context: Any):
        dt = pendulum.parse(context['ds'])

        tasks_to_execute = []

        if dt.weekday() in self.weekdays:
            tasks_to_execute.append(self.task_id_exclude)

        valid_task_ids = set(context["dag"].task_ids)   #все таск id

        invalid_task_ids = set(tasks_to_execute) - valid_task_ids

        if invalid_task_ids:
            raise AirflowException(
                f"Branch callable must return valid task_ids. "
                f"Invalid tasks found: {invalid_task_ids}"
            )
        self.skip_all_except(context['ti'], set(tasks_to_execute))  #метод SkipMixin

