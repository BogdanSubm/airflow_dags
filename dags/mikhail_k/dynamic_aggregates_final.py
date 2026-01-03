# dags/mikhail_k/dynamic_aggregates_final.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from operators.operator_s3_load_config_mikhail_k import S3LoadConfigOperator
from operators.operator_postgres_ensure_table_mikhail_k import PostgresEnsureTableOperator
from operators.operator_s3_export_csv_mikhail_k import S3ExportCSVOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from jinja2 import Template

def is_partition_empty(table_name: str, **context):
    ds = context["ds"]
    hook = PostgresHook("conn_pg")
    result = hook.get_first(f"SELECT 1 FROM {table_name} WHERE ds = '{ds}'::DATE LIMIT 1")
    is_empty = result is None
    print(f"[{table_name}] ds={ds} → {'ПАРТИЦИЯ ПУСТА → продолжаем' if is_empty else 'УЖЕ ЕСТЬ ДАННЫЕ → скип'}")
    return is_empty

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

    @task
    def process_aggregate(agg: dict):
        table = agg["table_name"]
        safe_id = table.replace(".", "_")

        # 1. Создаём таблицу
        ensure = PostgresEnsureTableOperator(
            task_id=f"ensure_table_{safe_id}",
            table_name=table,
            ddl_template=agg["table_ddl"],
            postgres_conn_id="conn_pg"
        )

        # 2. Проверяем — пустая ли партиция? (ShortCircuit — работает 100%)
        check_empty = ShortCircuitOperator(
            task_id=f"check_empty_{safe_id}",
            python_callable=is_partition_empty,
            op_kwargs={"table_name": table}
        )

        # 3. Загружаем данные (только если пусто)
        @task(task_id=f"load_data_{safe_id}")
        def load_data():
            sql = Template(agg["table_dml"]).render(
                table_name=table,
                ds="{{ ds }}",
                next_ds="{{ next_ds }}",
            )
            PostgresHook("conn_pg").run(sql)

        load = load_data()

        # 4. Экспорт (если нужно)
        if agg.get("need_to_export"):
            export_path = Template(agg["export_path"]).render(
                table_name=table.replace(".", "/")
            )
            export = S3ExportCSVOperator(
                task_id=f"export_{safe_id}",
                table_name=table,
                s3_path=export_path,
                postgres_conn_id="conn_pg",
                s3_conn_id="conn_s3"
            )
            ensure >> check_empty >> load >> export
        else:
            ensure >> check_empty >> load

    # === ЗАПУСК ВСЕХ АГРЕГАТОВ ===
    process_aggregate.expand(agg=load_config.output)

    # === КРАСИВАЯ РАМКА ===
    dag_start >> load_config >> process_aggregate.expand(agg=load_config.output) >> dag_end