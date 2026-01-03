# dags/operators/operator_s3_load_config_mikhail_k.py
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults
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
        hook = S3Hook(aws_conn_id=self.conn_id)
        content = hook.read_key(key=self.key, bucket_name=self.bucket)
        ns = {}
        exec(textwrap.dedent(content), {}, ns)
        config = ns.get("config")
        if not isinstance(config, list):
            raise ValueError("В agg_config.conf должна быть переменная config = [...]")
        return config