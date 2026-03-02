from airflow.decorators import dag, task

from datetime import datetime

@dag(
    start_date=datetime(2024, 9, 1),
    schedule='@once'
)
def lesson_3_task_flow_dag():

    @task()
    def python_test():
        return datetime.now()
    
    @task()
    def python_2_test(date: datetime):
        print(date)

    date = python_test()
    python_2_test(date)

lesson_3_task_flow_dag = lesson_3_task_flow_dag()