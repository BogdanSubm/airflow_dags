from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, SkipMixin

from typing import Any
import pendulum

class MyBrachOperator(BaseOperator, SkipMixin):
    
    def __init__(self, num_day, **kwargs):
        super().__init__(**kwargs)
        self.num_day = num_day
    
    def execute(self, context: Any):
        dt = pendulum.parse(context['ds'])

        tasks_to_execute = []

        if dt.day in self.num_day:
            tasks_to_execute.append('agg_data')
        
        valid_tasks_ids = set(context['dag'].task_ids)

        invalid_task_ids = set(tasks_to_execute) - valid_tasks_ids

        if invalid_task_ids:
            raise AirflowException(
                f"Branch callable must return valid task_ids. "
                f"Invalid tasks found: {invalid_task_ids}"
            )
        
        self.skip_all_except(context['ti'], set(tasks_to_execute))