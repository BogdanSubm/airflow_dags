from jinja2 import Template
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pendulum

from juventa.operators.juventa_postgresoperator import JuventaCustomPostgresOperator
from juventa.config.config import Jconfig
from juventa.export_to_c3 import save_table_to_c3


DEFAULT_ARGS = {
    'owner': 'juventa',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': pendulum.datetime(2026, 8, 11, tz="UTC")
}

#Даг, который запускается ежедневно (период можно поменять на еженедельно, ежемесячно)
with DAG(
    dag_id='juventa_finalpractice',
    #schedule='0 0 * * 1',  # Понедельник в 00:00 UTC
    schedule='@daily',
    #catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,

) as dag:
    dag_start = EmptyOperator(task_id='dag_start')

    dag_end = EmptyOperator(task_id='dag_end')


    task_groups = []
    #Считываю из конфигурации
    for c in Jconfig:
        table_name = c.get("table_name")
        table_ddl = c.get("table_ddl")
        table_dml = c.get("table_dml")
        table_export = c.get("need_to_export")

        # Рендерим только имя таблицы через Jinja
        ddl_rendered = Template(table_ddl).render(table_name=table_name)
        dml_rendered = Template(table_dml).render(table_name=table_name)


        with TaskGroup(group_id=f"group__{table_name}") as tg:
            #Создаем таблицу при необходимости

            task_created = JuventaCustomPostgresOperator(
                task_id=f'task_created_{table_name}',
                postgres_conn_id='conn_pg',
                dbname='etl',
                sql_query= ddl_rendered
            )
            #Удаляем если есть данные за запускаемый период
            #Для тестирования загружаем за 1 день {{ ds }}
            task_deleted = JuventaCustomPostgresOperator(
                task_id=f'task_deleted_{table_name}',
                postgres_conn_id='conn_pg',
                dbname='etl',
                sql_query= f"""
                    DELETE FROM {table_name}
                    WHERE date_start = %s AND date_end = %s
                """,
                parameters=(
                    '{{ ds }}',
                    '{{ ds }}'
                )
            )
            #Вставляем данные
            task_inserted = JuventaCustomPostgresOperator(
                task_id=f'task_inserted_{table_name}',
                postgres_conn_id='conn_pg',
                dbname='etl',
                sql_query= dml_rendered,
                parameters=(
                    '{{ ds }}',
                    '{{ ds }}',
                    '{{ ds }}',
                    '{{ ds }}',
                )
            )

            task_created >> task_deleted >> task_inserted

            #Сохраняем в объектное хранилище
            if table_export:
                task_export = PythonOperator(
                    task_id=f'task_export_{table_name}',
                    python_callable=save_table_to_c3,
                    op_kwargs={
                        'table_name': table_name,
                        'date_start': '{{ ds }}',
                        'date_end': '{{ ds }}',
                    }
                )
                task_inserted >> task_export

            task_groups.append(tg)


    dag_start >> task_groups >> dag_end

