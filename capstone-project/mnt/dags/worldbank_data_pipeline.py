from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone


default_args = {
    "owner": "Kan Ouivirach",
    "start_date": timezone.datetime(2022, 3, 20),
}
with DAG(
    "worldbank_data_pipeline",
    default_args=default_args,
    schedule_interval=None
) as dag:

    data_processing = SparkSubmitOperator(
        task_id="data_processing",
        conn_id="spark_conn",
        application="/usr/local/spark/app/data_processing.py",
        verbose=False
    )
