from operators.lotto_api_add_csv_operator import LottoApiAddCsvOperator
from airflow import DAG
import pendulum
from airflow import Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgresHook
from datetime import timedelta
from config.on_failure_callback_to_kakao import on_failure_callback_to_kakao

dataset_dags_dataset_producer = Dataset("dags_lotto_data")

with DAG(
    dag_id='dags_lotto_data',
    schedule='0 0 * * 6',
    start_date=pendulum.datetime(2023,10,1, tz='Asia/Seoul'),
    catchup=False,
    default_args={
        'on_failure_callback':on_failure_callback_to_kakao,
        'execution_timeout': timedelta(seconds=180)
    }
) as dag:
    
    start_task = BashOperator(
        task_id='start_task',
        outlets=[dataset_dags_dataset_producer],
        bash_command='echo "전 주 데이터 추가 작업 시작"'
    )

    tb_lotto_add = LottoApiAddCsvOperator(
        task_id='tb_lotto_add',
        outlets=[dataset_dags_dataset_producer],
        path='/opt/airflow/files/TbLottoAdd/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='TbLottoStatus.csv',
        file = '/opt/airflow/files/TbLottoStatus/TbLottoStatus.csv',
        time = '{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}'

    )

    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)

    insrt_postgresdb = PythonOperator(
        task_id='insrt_postgres',
        outlets=[dataset_dags_dataset_producer],
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'lotto_add_table',
                   'file_nm':'/opt/airflow/files/TbLottoAdd/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbLottoStatus.csv'}
    )

    def select_postgres(postgres_conn_id, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing
        
        postgres_hook = PostgresHook(postgres_conn_id)
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'hook select 수행'
                sql = 'select lotto_add_table.drwtNo1,lotto_add_table.drwNo2,lotto_add_table.drwtNo3, lotto_add_table.drwtNo4,lotto_add_table.drwNo5,lotto_add_table.drwNo6,lotto_add_table.bnsNo from lotto_add_table;'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    select_postgres_with_hook = PythonOperator(
        task_id='select_postgres_with_hook',
        python_callable=select_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgres-custom'}
    )
    

    finish_task = BashOperator(
        task_id='finish_task',
        outlets=[dataset_dags_dataset_producer],
        bash_command='echo "전 주 데이터 추가 작업 완료"'
    )

    start_task >> tb_lotto_add >> [insrt_postgresdb,select_postgres_with_hook] >> finish_task