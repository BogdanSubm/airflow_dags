#TRR_lesson2_dag.py 
from airflow.models.dag         import DAG
from airflow.operators.python   import PythonOperator
from airflow.operators.empty    import EmptyOperator
from airflow.operators.bash     import BashOperator

from datetime import datetime
# from src.utils import python_test_func # не хотчет он подтягивать 
# значит будет совать сюда

def python_test_func(animal:str, **context)-> None:
    print(animal)
    print(context['ds'])
    print(datetime.now())

with DAG (
    dag_id ='tolstyakoff_lesson2_dag',
    schedule = '@once',
    start_date = datetime(2026, 6, 11), 
) as dag:
    
    start_dag = EmptyOperator(task_id='start') #для маркировки начала и конца дага
    end_dag = EmptyOperator(task_id='end')

    test = PythonOperator (
        task_id='test',
        python_callable = python_test_func,
        op_kwargs = {
            "animal": "dog"
        },
    )
    
    bash_test = BashOperator(
        task_id='bash_test',
        bash_command='echo "Hello, from forest"',
    )

    bash_test2 = BashOperator(
        task_id='bash_test_2',
        bash_command = 'pip freeze',
    )

    start_dag >> test >> end_dag
    start_dag >> bash_test >>  bash_test2 >>end_dag
