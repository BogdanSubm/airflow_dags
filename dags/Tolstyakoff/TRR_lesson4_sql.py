# TRR_lesson4_dag.py 
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import psycopg2
from datetime import datetime

def query_func():
    # Получаем параметры подключения через BaseHook
    conn = BaseHook.get_connection('Tolstyakoff_conn')
    
    # Подключаемся к PostgreSQL через psycopg2
    pg_conn = psycopg2.connect(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        database=conn.schema
    )
    
    # SQL-запрос
    sql = """
    SELECT 
        category_name,
        count(p.product_id) AS cnt_prod,
        sum(p.units_in_stock) AS sum_units_in_stock
    FROM products p 
    JOIN categories c ON p.category_id = c.category_id 
    GROUP BY category_name
    """
    
    # Выполняем запрос
    cursor = pg_conn.cursor()
    cursor.execute(sql)
    records = cursor.fetchall()
    
    # Закрываем соединение
    cursor.close()
    pg_conn.close()
    
    # Выводим результат
    print("---- Query Results ----")
    for row in records:
        print(row)
    print("----------------------")
    print(f"Total rows: {len(records)}")
    
    return records

with DAG(
    dag_id='tolstyakoff_lesson4_sql_dag',
    schedule='@once',
    start_date=datetime(2026, 6, 11),
    catchup=False,
) as dag:
    
    start_dag = EmptyOperator(task_id='start')
    end_dag = EmptyOperator(task_id='end')

    DB_test = PythonOperator(
        task_id='DB_test',
        python_callable=query_func,
    )

    start_dag >> DB_test >> end_dag
