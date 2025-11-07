from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="test_spark_connection",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark"],
) as dag:
    spark_pi = SparkSubmitOperator(
        task_id="spark_pi",
        conn_id="spark_default",
        application="/opt/airflow/dags/spark_test.py",
        name="spark-pi",
        verbose=True,
    )