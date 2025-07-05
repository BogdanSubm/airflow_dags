from airflow.models import BaseOperator

from typing import Any
import pendulum

class MyBranchOperator(BaseOperator):
    
    def __init__(self, num_days, **kwargs):
        super().__init__(**kwargs)
        self.num_days = num_days
        
    def execute(self, context: Any):
        dt = pendulum.parse(context['ds'])

        if dt.day not in self.num_days:
            return 'upload_data'
        else:
            return 'agg_data'