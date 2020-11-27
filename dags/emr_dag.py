from airflow.operators import python_operator
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.utils.dates import days_ago


import boto3
import os
import logging

LOGGER = logging.getLogger("airflow.emr")

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

SPARK_STEPS = [
    {
        'Name': 'process-batch',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--packages',
                    'org.apache.spark:spark-avro_2.11:2.4.4', 
                    '--class',
                    'org.apache.spark.deploy.dotnet.DotnetRunner',
                    '--master',
                    'yarn',
                    's3://spark-app-vjal1251/jars/microsoft-spark-2-4_2.11-1.0.0.jar',
                    's3://spark-app-vjal1251/dll/emrapp.zip',
                    'emrApp',
                    's3a://spark-data-vjal1251/topics/orders/partition=0', 
                    's3a://spark-data-vjal1251/result']
        }
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'BatchFlow',
    'ReleaseLabel': 'emr-5.27.0',
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm4.large',
                'InstanceCount': 1,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}

def start():
    LOGGER.info(os.environ)
    LOGGER.info('start flow')


with DAG(
    dag_id='emr_job_flow_manual_steps_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1),
    tags=['example'],
) as dag:

    taks_start = python_operator.PythonOperator(
        task_id='start',
        python_callable=start)
        
    # [START howto_operator_emr_manual_steps_tasks]
    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
    )

    taks_start >> cluster_creator >> step_adder >> step_checker >> cluster_remover
    # [END howto_operator_emr_manual_steps_tasks]




