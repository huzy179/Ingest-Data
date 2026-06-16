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
    'retry_delay': timedelta(minutes=5),
}

def run_spark_maintenance():
    import docker
    client = docker.from_env()
    command = [
        "/opt/spark/bin/spark-submit",
        "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.clickhouse:clickhouse-jdbc:0.6.0,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:3.1.0",
        "/opt/spark/spark_apps/delta_maintenance.py"
    ]
    
    print("Starting Delta Lake maintenance Spark job...")
    exec_id = client.api.exec_create(container='banking-spark', cmd=command)['Id']
    output_stream = client.api.exec_start(exec_id, stream=True)
    
    for chunk in output_stream:
        print(chunk.decode('utf-8', errors='replace'), end='')
        
    status = client.api.exec_inspect(exec_id)
    exit_code = status['ExitCode']
    print(f"\nDelta maintenance job finished with exit code: {exit_code}")
    if exit_code != 0:
        raise Exception(f"Delta maintenance Spark job failed with exit code {exit_code}")

with DAG(
    'delta_lake_maintenance',
    default_args=default_args,
    description='Daily Delta Lake OPTIMIZE and VACUUM maintenance job',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False,
) as dag:

    run_maintenance = PythonOperator(
        task_id='run_delta_optimize_vacuum',
        python_callable=run_spark_maintenance,
    )
