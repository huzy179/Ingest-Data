from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'lakehouse_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def run_spark_job(script_name, packages_str):
    import docker
    client = docker.from_env()
    command = [
        "/opt/spark/bin/spark-submit",
        "--packages", packages_str,
        f"/opt/spark/spark_apps/{script_name}"
    ]
    
    print(f"Starting Batch Ingestion Job: {script_name} with command: {command}")
    exec_id = client.api.exec_create(container='banking-spark', cmd=command)['Id']
    output_stream = client.api.exec_start(exec_id, stream=True)
    
    for chunk in output_stream:
        print(chunk.decode('utf-8', errors='replace'), end='')
        
    status = client.api.exec_inspect(exec_id)
    exit_code = status['ExitCode']
    print(f"\nSpark job {script_name} finished with exit code: {exit_code}")
    if exit_code != 0:
        raise Exception(f"Spark job {script_name} failed with exit code {exit_code}")

with DAG(
    'lakehouse_batch_ingestion',
    default_args=default_args,
    description='Orchestrate Batch Ingestion from Postgres & MongoDB to MinIO',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
) as dag:

    ingest_postgres = PythonOperator(
        task_id='ingest_postgres_batch',
        python_callable=run_spark_job,
        op_kwargs={
            'script_name': 'batch_ingest_postgres.py',
            'packages_str': 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0'
        },
    )

    ingest_mongodb = PythonOperator(
        task_id='ingest_mongodb_batch',
        python_callable=run_spark_job,
        op_kwargs={
            'script_name': 'batch_ingest_mongodb.py',
            'packages_str': 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0'
        },
    )

    ingest_postgres >> ingest_mongodb
