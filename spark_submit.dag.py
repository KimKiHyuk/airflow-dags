from airflow.operators import python_operator
from datetime import datetime, timedelta
from airflow import DAG


    
def greeting(*args):
    import logging 
    logging.info(args)

with DAG('spark-submit-dag', 
    description='Python DAG', 
    schedule_interval='0 * * * *', 
    start_date=datetime(2020, 11, 18)) as dag:

    taks_1 = python_operator.PythonOperator(
        task_id='start',
        python_callable=greeting, op_args=['start'])
    taks_2_1 = python_operator.PythonOperator(
        task_id='mid_1',
        python_callable=greeting, op_args=['mid_1'])
    taks_2_2 = python_operator.PythonOperator(
        task_id='mid_2',
        python_callable=greeting, op_args=['mid_2'])
    taks_3 = python_operator.PythonOperator(
        task_id='end',
        python_callable=greeting, op_args=['mid_2'])

    taks_1 >> [taks_2_1, taks_2_2] >> taks_3
