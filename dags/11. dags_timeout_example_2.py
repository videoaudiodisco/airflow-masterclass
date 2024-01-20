from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from datetime import timedelta
from airflow.models import Variable

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_timeout_example_2',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None,
    dagrun_timeout=timedelta(minutes=1),
    default_args={
        'execution_timeout': timedelta(seconds=40),
        'email_on_failure': True,
        'email': email_lst
    }

    ## 중요! email_on_failure는 dag이 아니라 task에 걸려있는 것이다.
    # 따라서 이메일이 보내지려면 task가 실패가 되어야 하는데, 여기서는 skip이 뜨므로 실패가 아니어서 이메일이 안보내진다. 
) as dag:
    
    # 개별 task는 40초 미만이어서 timeout이 안되지만, dag 가 1분이므로 dag가 timeout 된다.
    
    bash_sleep_35 = BashOperator(
        task_id = 'bash_sleep_35',
        bash_command='sleep 35'
    )

    # dag timeout 시 이것이 실행중이므로 얘가 skipped
    bash_sleep_36 = BashOperator(
        trigger_rule='all_done',
        task_id = 'bash_sleep_36',
        bash_command='sleep 36'
    )

    # no status 상태로 계속 된다. 

    bash_go = BashOperator(
        task_id ='bash_go',
        bash_command='exit 0' #  success
    )


    bash_sleep_35 >> bash_sleep_36 >> bash_go