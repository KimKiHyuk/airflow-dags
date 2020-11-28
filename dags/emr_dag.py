from datetime import timedelta
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.utils.dates import days_ago
from airflow.operators import python_operator
from airflow.utils import trigger_rule

import os
import logging

LOGGER = logging.getLogger("airflow.dags.emr")

DEFAULT_ARGS = {
    'owner': 'key kim',
    'depends_on_past': False
}

SPARK_STEPS = [
    {
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
                    '--conf',
                    'spark.yarn.appMasterEnv.DOTNET_ASSEMBLY_SEARCH_PATHS=.',
                    's3://spark-app-vjal1251/jars/microsoft-spark-2-4_2.11-1.0.0.jar',
                    's3://spark-app-vjal1251/dll/emrapp.zip',
                    'emrApp',
                    's3a://spark-data-vjal1251/topics/orders/partition=0', 
                    's3a://spark-data-vjal1251/result']
        }
    }

]

JOB_FLOW_OVERRIDES = {
    'Name': 'ParseSQL',
    'ReleaseLabel': 'emr-5.31.0',
    "LogUri": 's3://aws-logs-417699346993-us-east-2/elasticmapreduce/',
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm4.large',
                'InstanceCount': 1,
            },
            {
                'Name': 'Slave node',
                'Market': 'SPOT',
                'InstanceRole': 'CORE',
                'InstanceType': 'm4.large',
                'InstanceCount': 2,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    "BootstrapActions": [
        {
            'Name': 'Install Microsoft.Spark.Worker',
            'ScriptBootstrapAction': {
                'Path': "s3://spark-app-vjal1251/install-worker.sh",
                'Args': [
                    "aws",
                    "s3://spark-app-vjal1251/Microsoft.Spark.Worker.netcoreapp3.1.linux-x64-1.0.0.tar.gz",
                    "/usr/local/bin"
                ]
            }
        },
    ],
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}


    
def dag_init():
    LOGGER.info(os.environ)
    LOGGER.info('start flow')


with DAG(
    dag_id='emr-dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1),
    tags=['sparm-emr'],
) as dag:

    dag_init = python_operator.PythonOperator(
        task_id='start',
        python_callable=dag_init)

    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='my_aws',
        emr_conn_id='emr_default',
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='my_aws',
        steps=SPARK_STEPS,
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='my_aws',
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='my_aws',
    )

    cluster_remover.trigger_rule = trigger_rule.TriggerRule.ALL_DONE


    dag_init >> cluster_creator >> step_adder >> step_checker >> cluster_remover