# dags/sensors/sensor_postgres_check_empty_partition_mikhail_k.py
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class PostgresCheckEmptyPartitionSensor(BaseSensorOperator):
    template_fields = ("table_name",)

    @apply_defaults
    def __init__(self, table_name, postgres_conn_id="conn_pg", **kwargs):
        super().__init__(poke_interval=60, timeout=600, mode="reschedule", **kwargs)
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id

    def poke(self, context):
        ds = context["ds"]
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        sql = f"SELECT 1 FROM {self.table_name} WHERE ds = '{ds}'::DATE LIMIT 1"
        result = hook.get_first(sql)
        is_empty = result is None
        self.log.info(f"[{self.table_name}] ds={ds} → {'пусто → продолжаем' if is_empty else 'уже заполнено → скип'}")
        return is_empty