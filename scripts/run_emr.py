import boto3
from dotenv import load_dotenv, find_dotenv
import os

if __name__ == "__main__":
    load_dotenv(find_dotenv())
    # emr-5.27.0
    #Ganglia 3.7.2, Spark 2.4.4, Zeppelin 0.8.1

    connection = boto3.client(
        'emr',
        region_name=os.environ['region_name'],
        aws_access_key_id=os.environ['aws_access_key_id'],
        aws_secret_access_key=os.environ['aws_secret_access_key'],
    )


    step = {
            'Name': 'process-avro-1',
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

    action = connection.add_job_flow_steps(JobFlowId=os.environ['job_flow_id'], Steps=[step])