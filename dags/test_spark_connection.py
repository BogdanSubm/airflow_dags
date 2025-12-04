from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="test_spark_connection",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    spark_pi = SparkSubmitOperator(
        task_id="spark_pi",
        application="/opt/airflow/dags/spark_test.py",
        name="spark-test",
        verbose=True,

        # ✨ Главное изменение
        deploy_mode="cluster",

        conf={
            "spark.master": "spark://172.20.20.15:7077",
            "spark.executor.memory": "512m",
            "spark.driver.memory": "512m",
        }
    )
