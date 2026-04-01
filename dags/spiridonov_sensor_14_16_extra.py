from airflow.sensors.base import BaseSensorOperator
from airflow.utils.state import State
from airflow.models import TaskInstance
from airflow.utils.session import provide_session

class MultiExternalTaskSensor(BaseSensorOperator):
    template_fields = ('external_tasks',)

    def __init__(self,external_tasks,**kwargs):
        super().__init__(**kwargs)
        self.external_tasks = external_tasks

    @provide_session
    def poke(self, context, session=None):
        execution_date = context['execution_date']
        for ext in self.external_tasks:
            dag_id = ext['dag_id']
            task_id = ext['task_id']

            self.log.info(f'Check {dag_id}.{task_id}')

            ti = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.task_id == task_id,
                TaskInstance.execution_date == execution_date,
            ).first()

            if not ti:
                self.log.info(f'No TaskInstance found for {dag_id}.{task_id}')
                return False

            if ti.state != State.SUCCESS:
                self.log.info(f'{dag_id}.{task_id} not ready: {ti.state}')
                return False

        self.log.info('All external tasks completed')
        return True