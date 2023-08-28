from operators.lotto_api_add_csv_operator import LottoApiAddCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_lotto_data_add',
    schedule='0 0 * * 6',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    tb_lotto_add = LottoApiAddCsvOperator(
        task_id='tb_lotto_add',
        dataset_nm='TblottoStatus',
        path='/opt/airflow/files/TbLottoAdd/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='TbLottoStatus.csv',
        file = '/opt/airflow/files/TbLottoStatus/TbLottoStatus.csv',
        time = '{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}'

    )

    tb_lotto_add