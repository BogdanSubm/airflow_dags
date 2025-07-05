from airflow.models import BaseOperator
from airflow.utils.context import Context
import pendulum

class MyBranchOperator(BaseOperator):
    """Кастомный оператор ветвления с правильной логикой пропуска"""
    
    def __init__(self, num_days, **kwargs):
        super().__init__(**kwargs)
        self.num_days = num_days
        
    def execute(self, context: Context):
        execution_date = context['ds']
        dt = pendulum.parse(execution_date)
        self.log.info(f"Checking day {dt.day} against {self.num_days}")
        
        if dt.day in self.num_days:
            self.log.info("Returning 'agg_data' as next task")
            return 'agg_data'
        else:
            self.log.info(f"Day {dt.day} not in {self.num_days}, skipping 'agg_data'")
            return 'upload_data'