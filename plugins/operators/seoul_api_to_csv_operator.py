from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd 

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path','file_name','base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        # self.endpoint = '76734a646d716b7235354c4e787a61/json/' + dataset_nm
        self.base_dt = base_dt

    def execute(self, context):
        import os
        
        print('endpoint 출력: ', self.endpoint)
        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        print('빈 df', total_row_df.shape)
        start_row = 1
        end_row = 1000
        while True:
            self.log.info(f'시작:{start_row}')
            self.log.info(f'끝:{end_row}')
            print('base_url', self.base_url)
            row_df = self._call_api(self.base_url, start_row, end_row)
            print(row_df.shape)
            total_row_df = pd.concat([total_row_df, row_df])
            print('새로운 df', total_row_df.shape)
            if len(row_df) < 1000:
                break
            else:
                start_row = end_row + 1
                print(start_row)
                end_row += 1000

        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json 

        headers = {'Content-Type': 'application/json',
                   'charset': 'utf-8',
                   'Accept': '*/*'
                   }

        request_url = f'{base_url}/{start_row}/{end_row}/'
        print('request_url 프린트', request_url)
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        response = requests.get(request_url, headers)
        print('response', response)
        contents = json.loads(response.text)
        print('contents', contents)

        key_nm = list(contents.keys())[0]
        print('key_nm', key_nm)
        row_data = contents.get(key_nm).get('row')
        print('row_data', row_data)
        row_df = pd.DataFrame(row_data)
        print('row_df', row_df.shape)

        return row_df



# from airflow.models.baseoperator import BaseOperator
# from airflow.hooks.base import BaseHook
# import pandas as pd 

# class SeoulApiToCsvOperator(BaseOperator):
#     template_fields = ('endpoint', 'path','file_name','base_dt')

#     # 유저가 SeoulApiToCsvOperator를 사용할때, 어떤 인자를 정의해줘야 하는지를 정한다.
#     # 예를 들어, dataset_nm, path, file_name, task_id만 넣어줘도 된다. 왜냐하면 첫3개는 정의되어있고, 정의되지 않은 task_id는 kwargs로 들어가기 때문에 

#     def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
#         super().__init__(**kwargs)
#         self.http_conn_id = 'openapi.seoul.go.kr'
#         self.path = path
#         self.file_name = file_name
#         self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
#         self.base_dt = base_dt


#     def execute(self, context):
#         import os

#         connection = BaseHook.get_connection(self.http_conn_id)
#         self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

#         total_row_df = pd.DataFrame()
#         start_row = 1
#         end_row=1000

#         while True:
#             self.log.info(f'시작:{start_row}')
#             self.log.info(f'끝 : {end_row}')
#             row_df = self._call_api(self.base_url, start_row, end_row)
#             total_row_df=pd.concat([total_row_df, row_df])
#             if len(row_df) < 1000:
#                 break
#             else:
#                 start_row=end_row+1
#                 end_row += 1000

#         # 파일 경로가 없으면 만들어주기 
#         if not os.path.exists(self.path):
#             os.system(f'mkdir -p {self.path}')
#         total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

#     def _call_api(self, base_url, start_row, end_row):
#         import requests
#         import json

#         headers = {'Content-Type': 'application/json',
#                 'charset': 'utf-8',
#                 'Accept': '*/*'
#                 }
        
#         request_url = f'{base_url}/{start_row}/{end_row}/'
#         if self.base_dt is not None:
#             request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
#         response=requests.get(request_url, headers)
#         contents=json.loads(response.text)

#         key_nm = list(contents.keys())[0]
#         row_data=contents.get(key_nm).get('row')
#         row_df=pd.DataFrame(row_data)

#         return row_df