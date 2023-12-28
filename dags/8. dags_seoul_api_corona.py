from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_seoul_api_corona',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''서울시 코로나19 확진자 발생동향'''
    tb_corona19_count_status = SeoulApiToCsvOperator(
        task_id='tb_corona19_count_status',
        dataset_nm='TbCorona19CountStatus',
        # dataset_nm='TbUseDaystatusView',
        path='/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='TbCorona19CountStatus.csv'
    )
    
    '''서울시 코로나19 백신 예방접종 현황'''
    tv_corona19_vaccine_stat_new = SeoulApiToCsvOperator(
        task_id='tv_corona19_vaccine_stat_new',
        dataset_nm='tvCorona19VaccinestatNew',
        # dataset_nm='TbUseDaystatusView',
        path='/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='tvCorona19VaccinestatNew.csv'
    )

    tb_corona19_count_status >> tv_corona19_vaccine_stat_new



# from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
# from airflow import DAG
# import pendulum

# with DAG(
#     dag_id='dags_seoul_api_corona',
#     schedule='0 7 * * *',
#     start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
#     catchup=False
# ) as dag:
    
#     '''서울시 코로나19 확진자 발생동향'''
#     tb_corona19_count_status = SeoulApiToCsvOperator(
#         task_id='tb_corona19_count_status',
#         # dataset_nm='TbCoronaCountStatus',
#         dataset_nm='cycleNewMemberRentInfoMonths',

#         # dags를 실행하는 주체는 워커 컨테이너인데 /opt/airflow/files 는 워커 컨테이너의 디렉토리
#         # 그런데 이렇게 path에 저장을 해도 컨테이너를 내렸다가 다시 올리면 저장한 파일이 사라진다. 
#         # 따라서, 컨테이너의 files 디렉토리를 본 서버(WSL, or window 등등)와 연결시켜주는 작업이 필요하다. --> yaml 파일을 수정 필요 volumes 부분
#         # /files:/opt/airflow/files 에서 : 왼쪽이 본 서버, 오른쪽이 컨테이너

#         # data_interval_end는 배치가 도는 날짜
#         path='/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
#         file_name='TbCorona19CountStatus.csv'
#     )

#     '''서울시 코로나19 백신 예방접종 현황'''
#     tv_corona19_vaccine_stat_new = SeoulApiToCsvOperator(
#         task_id='tv_corona19_vaccine_stat_new',
#         # dataset_nm='tvCorona19VaccinestatNew',
#         dataset_nm='cycleNewMemberRentInfoMonths',

#         path='/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
#         file_name='tvCorona19VaccinestatNew.csv'
#     )

#     tb_corona19_count_status >> tv_corona19_vaccine_stat_new
