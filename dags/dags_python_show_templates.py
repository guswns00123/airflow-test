from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_show_templates",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 8, 10, tz="Asia/Seoul"),
    catchup=False
) as dag:
    @task(task_id='python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    show_templates()