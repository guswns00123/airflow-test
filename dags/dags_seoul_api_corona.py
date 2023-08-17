from operators.seoul_api_to_csv_operator import SeoulApiTocsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id = 'dags_seoul_api_corona',
    schedule = '0 7 * * *',
    start_date = pendulum.datetime(2023, 8, 1, tz= 'Asia/Seoul'),
    catchup=False
) as dag:
    tb_corona19_count_status = SeoulApiTocsvOperator(
        task_id = 'tb_corona19_count_status',
        dataset_nm='TbCorona19CountStatus',
        path = '/opt/airflow-test/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul")  | ds_nodash}}',
        filename='TbCorona19CountStatus.csv'
    )

    tv_corona19_vaccine_stat_new = SeoulApiTocsvOperator(
        task_id = 'tv_corona19_vaccine_stat_new',
        dataset_nm= 'tvCorona19VaccinestatNew',
        path='/opt/airflow-test/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul) | ds_nodash}}',
        filename='tvCorona19VaccinestatNew.csv'
    )

    tb_corona19_count_status >> tv_corona19_vaccine_stat_new