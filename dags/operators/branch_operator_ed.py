from airflow.models import BaseOperator, SkipMixin
from airflow.utils.state import State

from typing import Any
import pendulum

class MyBrachOperator(BaseOperator, SkipMixin):
    
    def __init__(self, num_days, **kwargs):
        super().__init__(**kwargs)
        self.num_days = num_days
        
    def execute(self, context: Any):
        dt = pendulum.parse(context['ds'])

        if dt.day not in self.num_days:
            self.skip(context)
            return 'upload_data'
        else:
            return 'agg_data'