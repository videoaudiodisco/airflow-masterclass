from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

def on_failure_callback_to_slack(context):
    # context에는 미리 정의되어있는 template 변수들이 들어가있어서 꺼내쓸 수 있다.

    ti = context.get('ti') # task instance 를 객체로 저장
    dag_id = ti.dag_id
    task_id = ti.task_id
    err_msg = context.get('exception') # exception 키 값을 가져오기
    # end 이므로 실제로 batch가 돌아간 날짜를 구한다.
    batch_date = context.get('data_interval_end').in_timezone('Asia/Seoul')

    slack_hook = SlackWebhookHook(slack_webhook_conn_id = 'conn_slack_airflow_bot')

    text = '실패 알람'

    blocks = [
        {
            "type":"section",
            "text":{
                "type":"mrkdwn",
                "text":f"*{dag_id}.{task_id} 실패 알람*"
            }
        },
        {
            "type":"section",
            "fields": [
                {
                    "type":"mrkdwn",
                    "text":f"*배치 시간 : {batch_date}"
                },
                {
                    "type":"mrkdwn",
                    "text": f"*에러 내용* : {err_msg}"
                }
            ]
        }
    ]

    slack_hook.send(text=text, blocks = blocks)
    