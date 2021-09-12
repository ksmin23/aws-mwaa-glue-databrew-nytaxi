import datetime
import os
from airflow.models.baseoperator import ScheduleInterval
import boto3
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.contrib.operators.s3_delete_objects_operator import S3DeleteObjectsOperator
from airflow.hooks import S3_hook
import sys
import time
from airflow import DAG

DEFAULT_ARGS = {
    "owner": "admin",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

# S3 Prefix check. Function checks if s3 prefix exists and pushes variable True/False in xcom.
def check_prefix(**kwargs):
    s3_hook = S3_hook.S3Hook()
    prefix_status = s3_hook.check_for_prefix(bucket_name=bucket_name,
        prefix=s3_prefix+s3_partition+"="+today_date,
        delimiter='/')
    kwargs['ti'].xcom_push(key='value from xcom', value=prefix_status)
    print(prefix_status)
    return prefix_status

# s3 boto3 delete prefix . To be executed only if s3 prefix is found and is True.
def delete_prefix_boto(**kwargs):
    s3_cli = boto3.resource('s3')
    bucket = s3_cli.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=s3_prefix+s3_partition+"="+today_date):
        s3_cli.Object(bucket.name,obj.key).delete()

# Branch and call correct operator as per xcom variable
def branch(**kwargs):
    return 'prefix_' + str(kwargs['ti'].xcom_pull(task_ids='prefix_exists', key='value from xcom'))

# Custom operator to trigger Databrew job. This function utilizes boto3 client
def run_customer_job(**kwargs):
    job_name = kwargs['job_name']
    print("Starting Brew Job")

    # Trigger the Databrew job and monitor it’s state.
    run_id = glue_databrew_client.start_job_run(Name=job_name)
    state = glue_databrew_client.describe_job_run(Name=job_name,RunId=run_id['RunId'])

    # Keep polling every 30 seconds to see if there is a status change in Job run.
    if state:
        status = state['State']
        while status not in ['SUCCEEDED']:
            print("Sleeping")
            time.sleep(30)
            job_status = glue_databrew_client.describe_job_run(Name=job_name,RunId=run_id['RunId'])
            status = job_status['State']
            print("Checking the status. Current status is",status)
            if status in ['STOPPED', 'FAILED', 'TIMEOUT']:
                sys.exit(1)


# Initialization of variable
region_name = boto3.session.Session().region_name
glue_databrew_client = boto3.client('databrew', region_name=region_name)
today_date = str(datetime.date.today())

# Update the athena path here
athena_output = 's3://athena-query-result-location/' ###TODO: Update with your athena query result location

# Update the prefix of aggregated summary here
s3_prefix = 'aggregated_summary/'
s3_partition = 'partition_column'

# Update the bucket name where aggregated summary will be stored
bucket_name= 'output-bucket-name' ###TODO: Update with your s3 output-bucket-name

yellow_update_partition = """MSCK REPAIR TABLE nytaxi.yellow_taxi;"""
green_update_partition = """MSCK REPAIR TABLE nytaxi.green_taxi;"""
stage_table = """CREATE TABLE nytaxi.aggregate_staging
WITH (PARTITIONED_BY = ARRAY['partition_column'])
AS (
  SELECT 'yellow' AS taxi_type, * FROM nytaxi.yellow_taxi
  UNION
  SELECT 'green' AS taxi_type, * FROM nytaxi.green_taxi
);
"""

insert_aggregate_table = """INSERT INTO nytaxi.aggregate_summary(
  SELECT * FROM nytaxi.aggregate_staging
);
"""

drop_stage_table = """DROP TABLE nytaxi.aggregate_staging;"""

dag = DAG(
    dag_id="nytaxi-brew-job",
    default_args=DEFAULT_ARGS,
    default_view="graph",
    #schedule_interval="19 02 * * *", ####TODO: Change cron entry to schedule the job
    schedule_interval=None,
    start_date=datetime.datetime(2021, 3, 5), ###TODO: Modify start date accordingly
    catchup=False, ### set it to True if backfill is required.
    tags=["example"],
)

fork = BranchPythonOperator(
    task_id='Prefix_True_or_False',
    python_callable=branch,
    provide_context=True,
    dag=dag)

yellow_taxi = PythonOperator(task_id='yellow_taxi', python_callable=run_customer_job,
  op_kwargs={'job_name': 'yellow-taxi-job'}, dag=dag)
green_taxi = PythonOperator(task_id='green_taxi', python_callable=run_customer_job,
  op_kwargs={'job_name': 'green-taxi-job'}, dag=dag)
s3_prefix_exists = PythonOperator(task_id='prefix_exists', python_callable=check_prefix,
  op_kwargs={'s3_prefix': s3_prefix, 's3_partition': s3_partition, 'kwargs_date': today_date},
  provide_context=True, dag=dag)

true_task = DummyOperator(task_id='prefix_True', dag=dag)
false_task = DummyOperator(task_id='prefix_False', dag=dag)

delete_task = PythonOperator(task_id='prefix_delete', python_callable=delete_prefix_boto,
  op_kwargs={'s3_prefix': s3_prefix, 's3_partition': s3_partition, 'kwargs_date': today_date},
  dag=dag)

update_yellow_partition = AWSAthenaOperator(task_id="update_yellow_partition",
  query=yellow_update_partition, database='nytaxi', output_location=athena_output)

update_green_partition = AWSAthenaOperator(task_id="update_green_partition",
  query=green_update_partition, database='nytaxi', output_location=athena_output)

stage_table = AWSAthenaOperator(task_id="create_stage_table",
  query=stage_table, database='nytaxi', output_location=athena_output)

insert_aggregate_table = AWSAthenaOperator(task_id="insert_aggregate_table",
  trigger_rule='none_failed_or_skipped',
  query=insert_aggregate_table, database='nytaxi', output_location=athena_output)

drop_stage_table = AWSAthenaOperator(task_id="drop_stage_table",
  query=drop_stage_table, database='nytaxi', output_location=athena_output)

yellow_taxi >> update_yellow_partition >> stage_table >> s3_prefix_exists >> fork
green_taxi >> update_green_partition >> stage_table >> s3_prefix_exists >> fork
fork >> true_task >> delete_task >> insert_aggregate_table >> drop_stage_table
fork >> false_task >> insert_aggregate_table >> drop_stage_table

