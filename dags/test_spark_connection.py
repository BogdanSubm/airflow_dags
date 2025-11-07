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
        env_vars={
            "JAVA_HOME": "/usr/lib/jvm/temurin-8-jdk-amd64",
            "SPARK_HOME": "/opt/spark",
            "PATH": "/opt/spark/bin:/usr/lib/jvm/temurin-8-jdk-amd64/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "PYSPARK_PYTHON": "/usr/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
        },
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.executor.memory": "512m",
            "spark.driver.memory": "512m",
        },
    )
