from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

dateset_dags_dataset_producer_1 = Dataset('dags_dataset_producer_1')

with DAG(
    dag_id= 'dags_dataset_producer_1',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2023,4,1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[dateset_dags_dataset_producer_1],
        bash_command='echo "producer_1 수행완료"'
    )