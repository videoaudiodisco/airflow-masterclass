import pendulum
from dateutil.relativedelta import relativedelta
import os
import json
import requests
from airflow.models import Variable

REDIRECT_URL = 'https://example.com/oauth'

def _refresh_token_to_variable():
    client_id = Variable.get("kakao_client_secret")
    tokens = eval(Variable.et("kakao_tokens"))
    refresh_token = tokens.get('refresh_token')
    url = 'https://kauth.kakao.com/oauth/token'
    data = {
        "grant_type":"refresh_token",
        "client_id":f"{client_id}",
        "refresh_token":f"{refresh_token}"
    }

    response = requests.post(url, data=data) # 새로운 token 정보를 받아온다.
    rslt = response.json()
    new_access_token = rslt.get('access_token') # 없으면  null
    new_refresh_token = rslt.get('refresh_token') # 없으면 null

    if new_access_token:
        tokens['access_token'] = new_access_token
    
    if new_refresh_token:
        tokens['refresh_token'] = new_refresh_token

    now = pendulum.now('Asia/Seoul').strftime('%Y-%m-%d %H:%M:%S')
    tokens['updated'] = now
    os.system(f'airflow variables set kakao_tokens "{tokens}"') # airflow varialble 변경

    print('variable 업데이트 완료(key: kakao_tokens)')


def send_kakao_msg(talk_title: str, content:dict):
    '''
    content : {'title1':'content1', 'title2':'content2'...    }
    '''

    try_cnt = 0

    while True:
        ### get access token

        tokens = eval(Variable.get("kakao_tokens"))
        access_token = tokens.get('access_token')
        content_lst = []
        button_lst = []

        for title, msg in content.items():
            # 빈칸으로라도 넣어야 오류가 발생하지 않는다.
            content_lst.append({
                'title':f'{title}',
                'description':f'{msg}',
                'image_url':'',
                'image_width' : 40,
                'image_height' : 40,
                'link': {
                    'web_url' : '',
                    'mobile_web_url': ''
                }
            })
    
            button_lst.append({
                'title':'',
                'link': {
                    'web_url':'',
                    'mobile_web_url': ''
                }
            })
        list_data = {
            'object_type': 'list',
            'header_title': f'{talk_title}',
            'header_link': {
                'web_url': '',
                'mobile_web_url': '',
                'android_execution_params': 'main',
                'ios_execution_params': 'main'
            },
            'contents': content_lst,
            'buttons': button_lst
        }

        send_url = 'https://kapi.kakao.com/v2/api/talk/memo/default/send'
        headers = {
            "Authorization" : f'Bearer {access_token}'
        }
        data = {'template_object' : json.dump(list_data)}
        response = requests.post(send_url, headers=headers, data=data)
        print(f'try 횟수: {try_cnt}, response 상태: {response.status_code}')
        try_cnt += 1

        if response.status_code == 200: # normal
            return response.status_code
    
        elif response.status_code == 400: # 400 : bad request (잘못 요청시), 무조건 break 하도록 return
            return response.status_code
        elif response.status_code == 401 and try_cnt <= 2: # unauthorized(토큰 만료 등)
            _refresh_token_to_variable()
        elif response.status_code != 200 and try_cnt >= 3: # 400, 401 에러가 아닐겨우 3회 시도때 종료
            return response.status_code