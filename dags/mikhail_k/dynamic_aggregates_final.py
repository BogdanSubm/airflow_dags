# dags/dynamic_aggregates_final.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from operators.operator_s3_load_config_mikhail_k import S3LoadConfigOperator
from operators.operator_postgres_ensure_table_mikhail_k import PostgresEnsureTableOperator
from sensors.sensor_postgres_check_empty_partition_mikhail_k import PostgresCheckEmptyPartitionSensor
from operators.operator_s3_export_csv_mikhail_k import S3ExportCSVOperator
from jinja2 import Template

with DAG(
    dag_id="dynamic_aggregates_final",
    description="Финальная версия - полная динамика + сенсоры + защита от дублей",
    schedule_interval="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "mikhail_k",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["aggregates", "dynamic", "mikhail_k"],
    render_template_as_native_obj=True,
) as dag:
    
    dag_start = EmptyOperator(task_id='dag_start')

    dag_end = EmptyOperator(task_id='dag_end')
    
    # Загружаем конфиг из S3
    load_config = S3LoadConfigOperator(
        task_id="load_config_from_s3",
        bucket="mikhail-k",
        key="agg_config.conf",
        conn_id="conn_s3"
    )
    # Задача-шаблон: создаёт все нужные задачи для одного агрегата
    @task
    def process_one_aggregate(agg: dict):
        table = agg["table_name"]
        safe_id = table.replace(".", "_")

        # Создание таблицы
        ensure = PostgresEnsureTableOperator(
            task_id=f"ensure_table_{safe_id}",
            table_name=table,
            ddl_template=agg["table_ddl"],
            postgres_conn_id="conn_pg"
        )

        # Ожидание, что данные за ds пустые
        wait = PostgresCheckEmptyPartitionSensor(
            task_id=f"wait_empty_{safe_id}",
            table_name=table,
            postgres_conn_id="conn_pg"
        )

        # Загрузка данных
        @task(task_id=f"load_data_{safe_id}")
        def load_data(dml_template: str):
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            sql = Template(dml_template).render(
                table_name=table,
                ds="{{ ds }}",
                next_ds="{{ next_ds }}",
                prev_ds="{{ prev_ds }}",
            )
            PostgresHook("conn_pg").run(sql)

        load = load_data(agg["table_dml"])

        # Экспорт в S3, если нужно
        if agg.get("need_to_export"):
            export_path = Template(agg["export_path"]).render(
                table_name=table.replace(".", "/"),
                ds="{{ ds }}"
            )
            export = S3ExportCSVOperator(
                task_id=f"export_{safe_id}",
                table_name=table,
                s3_path=export_path,
                postgres_conn_id="conn_pg",
                s3_conn_id="conn_s3"
            )
            ensure >> wait >> load >> export
        else:
            ensure >> wait >> load

        return None 
    # Разворачиваем все агрегаты
    expanded_tasks = process_one_aggregate.expand(agg=load_config.output)

    dag_start >> load_config >> expanded_tasks >> dag_end