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
    'retry_delay': timedelta(minutes=1),
}

def run_spark_job(script_name, execution_date=None):
    import docker
    client = docker.from_env()
    command = [
        "/opt/spark/bin/spark-submit",
        "--packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.clickhouse:clickhouse-jdbc:0.6.0,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        f"/opt/spark/spark_apps/{script_name}"
    ]
    if execution_date:
        command.extend(["--execution-date", execution_date])
    
    print(f"Starting Spark job: {script_name} with args {command[3:]}")
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
    'lakehouse_spark_orchestration',
    default_args=default_args,
    description='Orchestrate PySpark Silver & Gold transformations',
    schedule_interval=timedelta(minutes=30),  # Run every 30 minutes
    catchup=False,
) as dag:

    run_silver = PythonOperator(
        task_id='run_silver_transformation',
        python_callable=run_spark_job,
        op_kwargs={
            'script_name': 'transform_silver.py',
            'execution_date': '{{ ds }}'
        },
    )

    run_gold = PythonOperator(
        task_id='run_gold_transformation',
        python_callable=run_spark_job,
        op_kwargs={'script_name': 'transform_gold.py'},
    )

    run_silver >> run_gold
