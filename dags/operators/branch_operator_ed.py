from airflow.models import BaseOperator

import pendulum

class MyBranchOperator(BaseOperator):
    
    def __init__(self, num_days, **kwargs):
        super().__init__(**kwargs)
        self.num_days = num_days
        
    def execute(self, context):
        dt = pendulum.parse(context['ds'])

        if dt.day in self.num_days:
            return 'agg_data'
        return 'upload_data'