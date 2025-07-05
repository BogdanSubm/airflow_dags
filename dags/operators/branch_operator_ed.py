from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, SkipMixin
from airflow.utils.state import State

from typing import Any
import pendulum

class MyBrachOperator(BaseOperator, SkipMixin):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
    def execute(self, context: Any):
        dt = pendulum.parse(context['ds'])

        if dt.day not in [1, 2, 5]:
            self.skip(context['ti'], context['dag_run'], ['agg_data'])
            context['ti'].set_state(State.SKIPPED)
        else:
            return 'agg_data'