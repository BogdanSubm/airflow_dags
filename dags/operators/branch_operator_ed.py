import pendulum

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, SkipMixin

class BranchOperator(BaseOperator, SkipMixin):

    def __init__(self, dt, num_days, **kwargs):
        super().__init__(**kwargs)
        self.dt = dt
        self.num_days = num_days
    
    def execute(self, context):
        dte = pendulum.parse(self.dt)
        day_of_month = dte.day

        tasks_to_execute = []

        if day_of_month in self.num_days:
            tasks_to_execute.append('agg_data')

        valid_task_ids = set(context['dag'].task_ids)

        invalid_task_ids = set(tasks_to_execute) - valid_task_ids

        if invalid_task_ids:
            raise AirflowException(
                f"Branch callable must return valid task_ids. "
                f"Invalid tasks found: {invalid_task_ids}"
            )
        
        self.skip_all_except(context['ti'], set(tasks_to_execute))