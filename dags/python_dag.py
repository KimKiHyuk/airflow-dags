from airflow.operators import python_operator
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.hooks.S3_hook import S3Hook

import boto3
import os
import logging

LOGGER = logging.getLogger("airflow.task")

    
def start():
    LOGGER.info(os.environ)
    LOGGER.info('start flow')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket("spark-app-vjal1251")
    for m in bucket.objects.all():
        print(m)

def create():
    LOGGER.info('create emr')

def execute():
    LOGGER.info('execute spark')

def destory():
    LOGGER.info('destroy emr')

def done():
    LOGGER.info('done flow')

with DAG('spark-submit-dag', 
    description='Python DAG', 
    schedule_interval=timedelta(days=1), 
    start_date=days_ago(0)) as dag:

    taks_start = python_operator.PythonOperator(
        task_id='start',
        python_callable=start)
    taks_create = python_operator.PythonOperator(
        task_id='create',
        python_callable=create)
    taks_execute = python_operator.PythonOperator(
        task_id='execute',
        python_callable=execute)
    taks_destroy = python_operator.PythonOperator(
        task_id='destroy',
        python_callable=destory)
    taks_done = python_operator.PythonOperator(
        task_id='done',
        python_callable=done)
    
    taks_start >> taks_create >> taks_execute >> taks_destroy >> taks_done
