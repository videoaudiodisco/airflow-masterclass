from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum
from config.on_failure_callbacks_to_slack import on_failure_callback_to_slack 


with DAG(
    dag_id = 'dags_on_failure_callback_to_slack',
    start_date=pendulum.datetime(2023,5,1, tz='Asia/Seoul'),
    schedule='0 * * * *',
    catchup=False,

    # 이것은 각각의 task로 전해진다.
    default_args={
        'on_failure_callback': on_failure_callback_to_slack,
        'execution_timeout' : timedelta(seconds=60) 
        # 60초 동안 각각의 task가 안끝나면 그 task는 실패가 된다. 
    }
) as dag:
    task_slp_90 = BashOperator(
        task_id='task_slp_90',
        bash_command='sleep 90' # 90초 후에 sleep이므로 실패가 된다. 
    )

    task_ext_1 = BashOperator(
        trigger_rule='all_done', # 위의 task가 실패이어도 실행되어야 하므로 
        task_id='task_ext_1',
        bash_command='exit 1' # 1이면 실패이다. 0만 성공
    )
    # 2 task가 모두 실패가 된다.
    task_slp_90 >> task_ext_1
