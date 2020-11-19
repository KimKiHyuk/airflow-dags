from airflow.operators import python_operator
from datetime import timedelta
from airflow import DAG

dag = DAG(dag_id='test',
        schedule_interval='*/5 * * * *',
        dagrun_timeout=timedelta(seconds=5))

    
def greeting(*args):
    import logging 
    logging.info(args)
python_callable=my_func, op_args=['one', 'two', 'three']
# An instance of an operator is called a task. In this case, the
# hello_python task calls the "greeting" Python function.
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
    python_callable=greeting, op_args=['end'])

taks_1 >> [taks_2_1, taks_2_2] >> taks_3
