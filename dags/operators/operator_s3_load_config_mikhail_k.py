# dags/operators/operator_s3_load_config_mikhail_k.py
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3
import json
from airflow.hooks.base import BaseHook
import textwrap

class S3LoadConfigOperator(BaseOperator):
    
    template_fields = ("bucket", "key")

    @apply_defaults
    def __init__(self, bucket, key, conn_id="conn_s3", **kwargs):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.key = key
        self.conn_id = conn_id

    def execute(self, context):
        # Получаем подключение (любого типа!)
        conn = BaseHook.get_connection(self.conn_id)

        # Если в Extra есть endpoint_url — берём оттуда, иначе из host
        extra = conn.extra_dejson if conn.extra_dejson else {}
        endpoint_url = extra.get("endpoint_url") or conn.host

        # Формируем сессию boto3
        session = boto3.session.Session()
        client = session.client(
            service_name='s3',
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            endpoint_url=endpoint_url.rstrip("/")  # убираем слеш в конце
        )

        self.log.info(f"Читаем s3://{self.bucket}/{self.key} через {endpoint_url}")

        try:
            obj = client.get_object(Bucket=self.bucket, Key=self.key)
            content = obj['Body'].read().decode('utf-8')
        except Exception as e:
            self.log.error(f"Не удалось прочитать объект: {e}")
            raise

        # Парсим Python-конфиг
        ns = {}
        exec(textwrap.dedent(content), {}, ns)
        config = ns.get("config")

        if not isinstance(config, list):
            raise ValueError("В agg_config.conf должна быть переменная config = [...]")

        self.log.info(f"Успешно загружено {len(config)} агрегатов из S3")
        return config