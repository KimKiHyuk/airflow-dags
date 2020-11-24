from airflow.operators import python_operator
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

import boto3
import os
import logging

LOGGER = logging.getLogger("airflow.emr")

def start():
    LOGGER.info('start flow')
    LOGGER.INFO(os.environ['AIRFLOW__AWS__ACCESS__KEY'])
    LOGGER.INFO(os.environ['AIRFLOW__AWS__SECRET__KEY'])
    LOGGER.INFO(os.environ['AIRFLOW__EMR__ID'])

def execute(cluster_id: str):
    connection = boto3.client(
        'emr',
        region_name='us-east-2',
        aws_access_key_id=os.environ['AIRFLOW__AWS__ACCESS__KEY'],
        aws_secret_access_key=os.environ['AIRFLOW__AWS__SECRET__KEY'],
    )

    if not cluster_id:
        LOGGER.info('cluster_id is empty') 
    else:
        LOGGER.info(f'run jobs on {cluster_id}')
        step = {
            'Name': 'process-avro',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--packages',
                        'org.apache.spark:spark-avro_2.11:2.4.6', 
                        '--class',
                        'org.apache.spark.deploy.dotnet.DotnetRunner',
                        '--master',
                        'yarn',
                        's3://spark-app-vjal1251/jars/microsoft-spark-2-4_2.11-1.0.0.jar',
                        's3://spark-app-vjal1251/dll/emrApp.dll', 
                        's3a://spark-data-vjal1251/topics/orders/partition=0', 
                        's3a://spark-data-vjal1251/result']
            }
        }

        action = connection.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])

def done():
    LOGGER.info('done flow')

with DAG('spark-emr-dag', 
    description='emr DAG', 
    schedule_interval=timedelta(days=1), 
    start_date=days_ago(0)) as dag:

    taks_start = python_operator.PythonOperator(
        task_id='start',
        python_callable=start)
    taks_execute = python_operator.PythonOperator(
        task_id='execute',
        python_callable=execute, op_kwargs={'cluster_id': ''})
    taks_done = python_operator.PythonOperator(
        task_id='done',
        python_callable=done)
    
    taks_start >> taks_execute >> taks_done




