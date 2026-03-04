from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from typing import Optional, Union, List, Dict, Any
import psycopg2 as pg

class CustomPostgresOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            sql: Union[str, List[str]],
            conn_id: str = 'conn_pg',
            *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = conn_id

    def execute(self, context: Any) -> None:
        rendered_sql = self.render_template(self.sql, context)

        connection = BaseHook.get_connection(self.conn_id)

        with pg.connect(
            dbname='etl',
            sslmode='disable',
            user=connection.login,
            password=connection.password,
            host=connection.host,
            port=connection.port,
            connect_timeout=600,
            keepalives_idle=600,
            tcp_user_timeout=600
        ) as conn:
            cursor = conn.cursor()
            cursor.execute(rendered_sql)
            conn.commit()
            cursor.close()




from airflow.operators.python import BranchPythonOperator

class CustomBranchOperator(BranchPythonOperator):
    def __init__(self, active_days, *args, **kwargs):
        self.active_days = active_days
        super().__init__(python_callable=self._check_day, *args, **kwargs)
    def _check_day(self, **context):
        day = context['execution_date'].day
        if day in self.active_days:
            return 'aggregate_data'
        return 'skip_day'



from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

#import CustomBranchOperator, CustomPostgresOperator

DEFAULT_ARGS = {
    'owner': 'spiridonov_a',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 13),
}

class WeekTemplates:
    @staticmethod
    def current_week_start(date) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        current_week_start = logical_dt - timedelta(days=logical_dt.weekday())
        return current_week_start.strftime("%Y-%m-%d")

    @staticmethod
    def current_week_end(date) -> str:
        logical_dt = datetime.strptime(date, '%Y-%m-%d')
        current_week_end = logical_dt + timedelta(days=6 - logical_dt.weekday())
        return current_week_end.strftime("%Y-%m-%d")

def upload_data(week_start, week_end, **context):
    import psycopg2 as pg
    from io import BytesIO
    import csv
    import boto3 as s3
    from botocore.client import Config
    import codecs

    sql_query = f"""
            SELECT * FROM spiridonov_les_13_agg
            WHERE date >= '{week_start}'::timestamp 
                  AND date < '{week_end}'::timestamp + INTERVAL '1 days';
        """

    connection = BaseHook.get_connection('conn_pg')

    with pg.connect(
            dbname='etl',
            sslmode='disable',
            user=connection.login,
            password=connection.password,
            host=connection.host,
            port=connection.port
    ) as conn:
        cursor = conn
        cursor.execute(sql_query)
        data = cursor.fetchall()

        file = BytesIO()
        writer = csv.writer(codecs.getwriter('utf-8')(file))
        writer.writerows(data)
        file.seek(0)

        s3_client = s3.client(
            's3',
            endpoint_url=BaseHook.get_connection('conn_s3').host,
            aws_access_key_id=BaseHook.get_connection('conn_s3').login,
            aws_secret_access_key=BaseHook.get_connection('conn_s3').password,
            config=Config(signature_version="s3v4"),
        )

        s3_client.put_object(
            Body=file,
            Bucket='default_storage',
            Key=f"spiridonov_{week_start}_{context['ds']}.csv"
        )

ACTIVE_DAYS = [1, 2, 5]

with DAG(
    dag_id='spiridonov_les_13_agg',
    tags = ['spiridonov', '13'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    catchup=True,
    max_active_runs=1,
    user_defined_macros={
        'current_week_start': WeekTemplates.current_week_start,
        'current_week_end': WeekTemplates.current_week_end,
    },
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    skip_day  = EmptyOperator(task_id='skip_day')

    check_day = CustomBranchOperator(
        task_id='check_day',
        active_days=ACTIVE_DAYS,
    )

    aggregate = CustomPostgresOperator(
        task_id='aggregate_data',
        sql="""
            INSERT INTO spiridonov_les_13_agg
            SELECT lti_user_id,
                   attempt_type,
                   COUNT(1)                                         AS cnt_attempt,
                   COUNT(CASE WHEN is_correct THEN 1 ELSE NULL END) AS cnt_correct,
                   '{{ current_week_start(ds) }}'::timestamp
            FROM spiridonov_admin_table_8
            WHERE created_at >= '{{ current_week_start(ds) }}'::timestamp 
                      AND created_at < '{{ current_week_end(ds) }}'::timestamp + INTERVAL '1 days'
            GROUP BY lti_user_id, attempt_type;
            """
    )

    upload = PythonOperator(
        task_id = 'upload_data_to_s3',
        python_callable=upload_data,
        op_kwargs = {
            'week_start': '{{ current_week_start(ds) }}',
            'week_end': '{{ current_week_end(ds) }}',
        }
    )

    start >> check_day
    check_day >> [aggregate, skip_day ]
    aggregate >> upload >> end
    skip_day  >> end
