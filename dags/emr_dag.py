#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This is an example dag for a AWS EMR Pipeline.
Starting by creating a cluster, adding steps/operations, checking steps and finally when finished
terminating the cluster.
"""
from datetime import timedelta
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.utils.dates import days_ago
from airflow.operators import python_operator

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
    'Name': 'ParseSQL',
    'ReleaseLabel': 'emr-5.27.0',
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
    schedule_interval=timedelta(hours=1),
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

    dag_init >> cluster_creator >> step_adder >> step_checker >> cluster_remover