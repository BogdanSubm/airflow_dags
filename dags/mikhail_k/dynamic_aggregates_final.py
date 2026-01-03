# dags/mikhail_k/dynamic_aggregates_final.py
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from jinja2 import Template
import boto3
from airflow.hooks.base import BaseHook

def load_and_run(**context):
    conn = BaseHook.get_connection("conn_s3")
    session = boto3.session.Session()
    s3 = session.client(
        's3',
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        endpoint_url=conn.host
    )
    obj = s3.get_object(Bucket="mikhail-k", Key="agg_config.conf")
    content = obj['Body'].read().decode()
    exec(content, globals())
    
    for agg in config:
        table = agg["table_name"]
        
        # ensure
        PostgresHook("conn_pg").run(Template(agg["table_ddl"]).render(table_name=table))
        
        # check empty
        has_data = PostgresHook("conn_pg").get_first(
            f"SELECT 1 FROM {table} WHERE ds = '{{{{ ds }}}}'::DATE LIMIT 1"
        )
        if has_data:
            print(f"{table} уже заполнена за {{ds}} — скип")
            continue
            
        # load
        sql = Template(agg["table_dml"]).render(table_name=table)
        PostgresHook("conn_pg").run(sql)
        
        # export
        if agg.get("need_to_export"):
            path = Template(agg["export_path"]).render(table_name=table.replace(".", "/"))
            bucket, key = path.replace("s3://", "").split("/", 1)
            csv = PostgresHook("conn_pg").get_pandas_df(
                f"COPY (SELECT * FROM {table} WHERE ds = '{{{{ ds }}}}'::DATE) TO STDOUT WITH CSV HEADER"
            ).to_csv(index=False).encode()
            s3.put_object(Bucket=bucket, Key=key, Body=csv)

with DAG(
    dag_id="dynamic_aggregates_final",
    schedule_interval="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "mikhail_k"},
    tags=["simple", "working"]
) as dag:
    
    run = PythonOperator(
        task_id="run_all_aggregates",
        python_callable=load_and_run
    )
    
    EmptyOperator(task_id="start") >> run >> EmptyOperator(task_id="end")