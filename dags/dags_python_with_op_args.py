from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from common.common_fuc import regist

with DAG(
    dag_id="dags_python_with_op_args",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag: 
    task_regist = PythonOperator(
        task_id = 'task_regist',
        python_callable=regist,
        op_args = ['hjyoo', 'man', 'kr', 'seoul']
    )

    task_regist