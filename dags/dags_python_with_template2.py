from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 8, 10, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def python_function1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    py_t1 = PythonOperator(
        task_id = 'py_t1',
        python_callable = python_function1,
        op_kwargs={'start_date':'{{data_interval_start | ds}}', 'end_date': '{{data_interval_end | ds}}'}
    )

    @task(task_id = 'py_t2')
    def python_function2(**kwargs):
        print(kwargs)
        print('ds:' + kwargs['ds'])
        print('ts: ' + kwargs['ts'])
        print('data_interval_start:' + str(kwargs['data_interval_start']))

    py_t1 >> python_function2()