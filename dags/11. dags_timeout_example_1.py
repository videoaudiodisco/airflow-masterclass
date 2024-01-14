# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from datetime import timedelta
from airflow.models import Variable

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id = 'dags_timeout_example_1',
    start_date= pendulum.datetime(2023,5,1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None,
    dagrun_timeout=timedelta(minutes=1), # dag 전체의 timeout은 1분
    default_args={
        'execution_timeout': timedelta(seconds=20), # 각 task의 timeout은 20초
        'email_on_failure':True,
        'email': email_lst
    }
) as dag:
    bash_sleep_30 = BashOperator(
        task_id = 'bash_sleep_30',
        bash_command='sleep 30', # 20<30 이므로  fail
    )

    bash_sleep_10= BashOperator(
        # 선행 task가 성공이든 실패든 실행되기만 하면 다음 task 실행
        trigger_rule='all_done', 

        task_id = 'bash_sleep_10',
        bash_command='sleep 10', # 성공
    )

    bash_sleep_30 >> bash_sleep_10

    # 2 tasks : 30 + 10 < 1 min --> therefore, the dag itself is success