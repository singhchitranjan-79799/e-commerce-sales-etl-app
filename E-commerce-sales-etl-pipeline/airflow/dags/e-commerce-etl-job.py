from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    "owner": "chitranjan",
    "depends_on_past": False,
    "retries": 3,
}

with DAG(
    dag_id="run_ecommerce_glue_jobs",
    default_args=default_args,  
    start_date=datetime(2026, 9, 5),
    schedule=None,
    catchup=False,
    tags=["glue"]
) as dag:

    glue_job_1 = GlueJobOperator(
        task_id="bronze_etl",
        job_name="bronze_job",
        aws_conn_id="aws_connection",
        region_name="us-east-1",
        wait_for_completion=True,
    )

    glue_job_2 = GlueJobOperator(
        task_id="silver_etl",
        job_name="silver_job",
        aws_conn_id="aws_connection",
        region_name="us-east-1",
        wait_for_completion=True,
    )

    glue_job_1 >> glue_job_2