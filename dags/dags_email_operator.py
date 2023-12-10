from airflow import DAG
from airflow.operators.email import EmailOperator
import datetime
import pendulum

with DAG(
    dag_id="dags_email_operator",
    schedule='0 8 ` * *',
    start_date=pendulum.datetime(2023,3,1, tz="Asia/Seoul"),
    catchup=False
    ) as dag:

    send_email = EmailOperator(
        task_id = "send_email",
        to='gyungyoonpark@gmail.com',
        subject='airflow 성공!',
        html_contents = 'airflow 작업이 완료!'
    )
