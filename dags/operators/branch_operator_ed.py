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

        if dt.day not in self.num_day:
            self.skip(context['ti'], context['dag_run'], ['agg_data'])