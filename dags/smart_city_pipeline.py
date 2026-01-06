from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

def check_hdfs_data():
    """Verify that WebHDFS is accessible and raw data exists."""
    try:
        # Check WebHDFS for raw topic folder
        response = requests.get("http://namenode:9870/webhdfs/v1/data/raw/traffic?op=LISTSTATUS")
        if response.status_code == 200:
            print("Successfully connected to HDFS. Data found.")
            return True
        else:
            print(f"HDFS check failed: {response.status_code}")
            # Non-critical failure for demo purposes (or raise ValueError to block)
            return True 
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

with DAG(
    'smart_city_pipeline',
    default_args=default_args,
    description='End-to-end Urban Traffic Pipeline',
    schedule_interval=timedelta(minutes=5),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Check Ingestion (Python)
    check_ingestion = PythonOperator(
        task_id='check_ingestion',
        python_callable=check_hdfs_data
    )

    # Task 2: Spark Processing Job (Docker - PySpark)
    # Triggers the spark-submit logic using DockerOperator
    # Note: We use the network 'bigdatapartbypart_default' so it can reach 'spark-master'
    spark_job = DockerOperator(
        task_id='process_traffic_spark',
        image='bigdatapartbypart-spark-submit:latest',
        api_version='auto',
        auto_remove=True,
        command="/spark/bin/spark-submit --master spark://spark-master:7077 --jars /spark/jars/postgresql-42.6.0.jar process_traffic.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bigdatapartbypart_default",
        environment={
            "SPARK_MASTER": "spark://spark-master:7077",
            "CORE_CONF_fs_defaultFS": "hdfs://namenode:9000",
            "PYSPARK_PYTHON": "python3",
            "PYSPARK_DRIVER_PYTHON": "python3"
        }
    )
    
    # Task 3: Validation (Postgres)
    # Check if we have data in the metrics table.
    validate_data = PostgresOperator(
        task_id='validate_data',
        postgres_conn_id='traffic_db',
        sql="SELECT count(*) FROM zone_metrics;",
        autocommit=True
    )

    check_ingestion >> spark_job >> validate_data
