from datetime import datetime, timedelta
import requests
import psycopg2
import json
import hashlib
import pandas as pd
import boto3

from io import StringIO

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook


API_URL = "https://b2b.itresume.ru/api/statistics"

DEFAULT_ARGS = {
    "owner": "reylife",
    "start_date": datetime(2026, 6, 12),
    "retries": 2
}


def extract_data(ds, **kwargs):
    end_date = ds

    start_date = (
        datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=7)
    ).strftime("%Y-%m-%d")

    response = requests.get(
        API_URL,
        params={
            "client": "Skillfactory",
            "client_key": "M2MGWS",
            "start": start_date,
            "end": end_date
        }
    )

    data = response.json()

    conn = BaseHook.get_connection("conn_pg")

    db = psycopg2.connect(
        host=conn.host,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )

    cur = db.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reylife_raw (
        hash text PRIMARY KEY,
        payload jsonb
    )
    """)

    for row in data:

        row_hash = hashlib.md5(
            json.dumps(row).encode()
        ).hexdigest()

        cur.execute("""
        INSERT INTO reylife_raw(hash, payload)
        VALUES (%s,%s)
        ON CONFLICT DO NOTHING
        """, (
            row_hash,
            json.dumps(row)
        ))

    db.commit()
    db.close()


def aggregate_data():

    conn = BaseHook.get_connection("conn_pg")

    db = psycopg2.connect(
        host=conn.host,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )

    cur = db.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reylife_agg (
        total_rows int,
        unique_users int,
        correct_answers int
    )
    """)

    cur.execute("DELETE FROM reylife_agg")

    cur.execute("""
    INSERT INTO reylife_agg

    SELECT

        COUNT(*) as total_rows,

        COUNT(DISTINCT payload->>'lti_user_id'),

        COUNT(*) FILTER (
            WHERE payload->>'is_correct'='true'
        )

    FROM reylife_raw
    """)

    db.commit()
    db.close()


def export_csv():

    pg = BaseHook.get_connection("conn_pg")
    s3 = BaseHook.get_connection("conn_s3")

    db = psycopg2.connect(
        host=pg.host,
        dbname=pg.schema,
        user=pg.login,
        password=pg.password,
        port=pg.port
    )

    df = pd.read_sql(
        "SELECT * FROM reylife_agg",
        db
    )

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    client = boto3.client(
        "s3",
        endpoint_url=s3.host,
        aws_access_key_id=s3.login,
        aws_secret_access_key=s3.password
    )

    client.put_object(
        Bucket="reylife-bucket",
        Key="result.csv",
        Body=csv_buffer.getvalue()
    )

    db.close()


with DAG(
    dag_id="reylife_dag",
    schedule="@weekly",
    catchup=False,
    default_args=DEFAULT_ARGS
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    aggregate = PythonOperator(
        task_id="aggregate_data",
        python_callable=aggregate_data
    )

    export = PythonOperator(
        task_id="export_csv",
        python_callable=export_csv
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> extract >> aggregate >> export >> end