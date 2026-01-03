# dags/mikhail_k/dynamic_aggregates_final.py
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

    dag_start = EmptyOperator(task_id="dag_start")
    dag_end = EmptyOperator(task_id="dag_end", trigger_rule="none_failed_min_one_success")

    load_config = S3LoadConfigOperator(
        task_id="load_config_from_s3",
        bucket="mikhail-k",
        key="agg_config.conf",
        conn_id="conn_s3"
    )

    # ЭТИ ЗАДАЧИ СОЗДАЮТСЯ НА УРОВНЕ DAG — НЕ ВНУТРИ @task
    @task
    def ensure_table(agg: dict):
        op = PostgresEnsureTableOperator(
            task_id=f"ensure_table_{agg['table_name'].replace('.', '_')}",
            table_name=agg["table_name"],
            ddl_template=agg["table_ddl"],
            postgres_conn_id="conn_pg"
        )
        op.execute({})

    @task
    def wait_empty(agg: dict):
        op = PostgresCheckEmptyPartitionSensor(
            task_id=f"wait_empty_{agg['table_name'].replace('.', '_')}",
            table_name=agg["table_name"],
            postgres_conn_id="conn_pg"
        )
        op.execute({})

    @task
    def load_data(agg: dict):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        sql = Template(agg["table_dml"]).render(
            table_name=agg["table_name"],
            ds="{{ ds }}",
            next_ds="{{ next_ds }}",
        )
        PostgresHook("conn_pg").run(sql)

    @task
    def export_if_needed(agg: dict):
        if not agg.get("need_to_export"):
            return
        export_path = Template(agg["export_path"]).render(
            table_name=agg["table_name"].replace(".", "/")
        )
        op = S3ExportCSVOperator(
            task_id=f"export_{agg['table_name'].replace('.', '_')}",
            table_name=agg["table_name"],
            s3_path=export_path,
            postgres_conn_id="conn_pg",
            s3_conn_id="conn_s3"
        )
        op.execute({})

    # ДИНАМИЧЕСКОЕ РАЗВОРАЧИВАНИЕ
    ensure_tasks = ensure_table.expand(agg=load_config.output)
    wait_tasks = wait_empty.expand(agg=load_config.output)
    load_tasks = load_data.expand(agg=load_config.output)
    export_tasks = export_if_needed.expand(agg=load_config.output)

    # ЗАВИСИМОСТИ
    ensure_tasks >> wait_tasks >> load_tasks >> export_tasks

    # КРАСИВАЯ РАМКА
    dag_start >> load_config >> ensure_tasks >> dag_end