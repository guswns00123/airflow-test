from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from common.common_fuc import get_sftp

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "example2"],
) as dag: 
    task_get_sftp = PythonOperator(
        task_id = 'task_get_sftp',
        python_callable=get_sftp
    )

    task_get_sftp