# TRR_lesson4_dag.py 
# ОТрабатываем подключения
from airflow.models.dag         import DAG
from airflow.operators.python   import PythonOperator
from airflow.operators.empty    import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

def query_func ():
    hook = PostgresHook(postgres_conn_id='Tolstyakoff_conn')

    # SQL-запрос
    sql = """
    SELECT 
	  category_name,
	  count(p.product_id) AS cnt_prod ,
	  sum (p.units_in_stock) AS sum_units_in_stoc
    FROM products p JOIN categories c ON p.category_id = c.category_id 
    GROUP BY category_name
    """
    # 3. Выполняем запрос и получаем результат
    #    hook.get_records() возвращает список кортежей.
    records = hook.get_records(sql)
    
    # 4. Выводим результат в консоль (логи Airflow)
    print(f"SQL Query: {sql}")
    print("---- Query Results ----")
    for row in records:
        print(row)
    print("----------------------")
    
    # При желании можно вернуть результат для использования в других тасках (через XCom)
    return records

with DAG (
    dag_id ='tolstyakoff_lesson4_dag',
    schedule = '@once',
    start_date = datetime(2026, 6, 11), 
    catchup=False,  # 👈 чтобы не гонял впустую
) as dag:
    
    start_dag = EmptyOperator(task_id='start') #для маркировки начала и конца дага
    end_dag = EmptyOperator(task_id='end')

    DB_test = PythonOperator (
        task_id='DB_test',
        python_callable = query_func,
    )

    start_dag >> DB_test >> end_dag
