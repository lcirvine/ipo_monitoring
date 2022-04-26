from pg_connection import pg_connection
import pandas as pd
from datetime import datetime, timedelta
import confuse
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import json
import time
import os
import sys
from io import StringIO
from logging_ipo_dates import logger

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

config = confuse.Configuration('wf_api', __name__)
config.set_file('config.yaml')


def get_upcoming_ipos() -> pd.DataFrame:
    conn = pg_connection()
    query = """SELECT DISTINCT 
    COALESCE(pp.iconum, em.iconum) AS iconum
    ,ipo.company_name
    ,pp.master_deal
    ,ipo.ticker
    ,ipo.exchange
    ,ipo.ipo_date
    ,ipo.price
    ,ipo.price_range
    ,ipo.shares_offered
    ,ipo.notes
    --,ipo.time_added
FROM all_ipos ipo
LEFT JOIN entity_mapping em ON ipo.company_name = em.company_name
LEFT JOIN peo_pipe pp ON ipo.company_name = pp.company_name OR em.iconum = pp.iconum
WHERE 
    ipo.ipo_date >= CURRENT_DATE
    AND (
        (pp.trading_date != ipo.ipo_date OR pp.trading_date IS NULL)
        OR (pp.price != ipo.price OR pp.price IS NULL)
    )"""
    try:
        df = pd.read_sql_query(query, conn, parse_dates=['ipo_date'])
        logger.info(f"{len(df)} upcoming IPOs returned from query")
        # columns must look EXACTLY like they do when downloading bulk upload template!
        # column names are uppercase except for priority which is 'i_priority'
        df.columns = [c.upper() for c in df.columns]
        cols = df.columns.to_list()
        cols.insert(0, 'i_priority')

        # i_priority MUST be given as an integer, so the ID rather than the name (see mapping below)!
        # IPOs listing in the next 3 days should be high priority, the rest should be medium
        priority_mapping = {
            'Critical': 31,
            'High': 30,
            'Medium': 20,
            'Low': 10,
            'None': 0
        }
        df.loc[df['IPO_DATE'] <= (datetime.utcnow() + timedelta(days=3)), 'i_priority'] = priority_mapping['High']
        df['i_priority'].fillna(priority_mapping['Medium'], inplace=True)

        # Numeric columns CAN NOT be n/a or null and CAN NOT be floats (i.e. 1.23)!
        # changing columns to int and filling NA with 0
        for col in ['ICONUM', 'MASTER_DEAL', 'SHARES_OFFERED', 'i_priority']:
            df[col] = df[col].astype('Int64', errors='ignore')
            df[col].fillna(0, inplace=True)
        df = df[cols]

        # when saving the CSV DO NOT encode it!
        # the upload is in binary and Genesys does not know how to interpret the BOM
    finally:
        conn.close()
    return df


class GenesysAPI:
    def __init__(self, content_set: str, user_id: int, environment: str = 'stg'):
        self.content_set = content_set.lower()
        self.user_id = user_id
        self.environment = environment.lower()
        if self.environment in ('prod', 'production'):
            self.base_url = 'https://genesys.factset.com'
        else:
            self.base_url = 'https://genesys-stg.factset.com'
        if self.content_set == 'loans':
            self.token = self.authenticate(api_key=config['api']['genesys']['loans_key'].get())
        elif self.content_set in ('peopipe', 'peo-pipe', 'peo_pipe'):
            self.token = self.authenticate(api_key=config['api']['genesys']['peopipe_key'].get())
        else:
            raise ValueError(f"Unknown content set, no API key available for {content_set}")
        self.headers = {'accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': f'Bearer {self.token}'}
        # self.conn = pg_connection()

    def authenticate(self, api_key, user_id=None):
        """Authenticate API key for a user. user_id defaults to the user_id of the class
        but can be used for a different user if provided.

        :param user_id: Identifies the user
        :param api_key: API key, file an RPD for the workflow engineers if you do not have one
        :return: Access token
        """
        endpoint_token = f"{self.base_url}/api/v1/auth/token"
        if user_id is None:
            user_id = self.user_id
        payload = {
            'user_id': user_id,
            'api_key': api_key
        }
        headers = {'accept': 'application/json', 'Content-Type': 'application/json'}
        res = requests.post(url=endpoint_token, json=payload, headers=headers, verify=False)
        assert res.ok, f"Error authenticating\n{res.status_code}\n{res.text}"
        return json.loads(res.text)['data']['access_token']

    def workflows(self) -> pd.DataFrame:
        """Return a dataframe with the workflows the user has access to

        :return: Dataframe with the id and wf_name as columns
        """
        res = requests.get(url=f"{self.base_url}/api/v1/workflows", headers=self.headers, verify=False)
        assert res.ok, f"Error retrieving workflows\n{res.status_code}\n{res.text}"
        res_json = json.loads(res.text)
        return pd.json_normalize(res_json['data'])

    def files(self, file_id: str, process_name: str) -> dict:
        """Checks the status of a process like bulk upload, bulk feed or bulk operation

        :param file_id: The csv_uuid from the presigned URL
        :param process_name: Valid processes are 'BulkUpload', 'BulkOpsStatus', 'BulkFeed'.
            The process name must match the process name used when getting the presigned URL
        :return: JSON data from response
        """
        endpoint = f"{self.base_url}/api/v1/files/{file_id}"
        valid_processes = ('BulkUpload', 'BulkOpsStatus', 'BulkFeed')
        if process_name not in valid_processes:
            raise ValueError(f"Invalid process: {process_name}, valid process names are {', '.join(valid_processes)}")
        res = requests.get(url=endpoint, params={'process_name': process_name}, headers=self.headers, verify=False)
        assert res.ok, f"Error\n{res.status_code}\n{res.text}"
        res_json = json.loads(res.text)
        return res_json

    def wf_properties(self, wf_id: int) -> dict:
        """Returns a dictionary with properties of the workflow, useful for filtering tasks returned.

        Following properties will be returned:
        COLUMNSRELATION, FEED_CONF, GETNEXTOPTIONS, OUTBOUND_ACTIONS,
        OUTBOUND_CONF, OUTBOUND_RULES, PRIORITY, RPD, SOFTCOLUMNS, STATUS, UNIQUE_CHECK, VIEWCOLUMN, WORKFLOW

        :param wf_id: Integer identifying the workflow
        :return: Dictionary with workflow properties
        """
        res = requests.get(url=f"{self.base_url}/api/v1/workflows/{wf_id}/properties", headers=self.headers, verify=False)
        assert res.ok, f"Error retrieving workflow properties\n{res.status_code}\n{res.text}"
        res_json = json.loads(res.text)
        return res_json['data']

    def wf_columns(self, wf_id: int) -> pd.DataFrame:
        """Creates a dataframe with the columns in a workflow.
        The API returns three dictionaries, one for HARDCOLUMNS, SOFTCOLUMNS, and VIEWCOLUMN. This method will
        combine the three dictionaries into one dataframe.

        :param wf_id: Integer identifying the workflow
        :return: dataframe
        """
        endpoint = f"{self.base_url}/api/v1/configs/workflowviews"
        res = requests.get(url=endpoint, params={'wfId': wf_id}, headers=self.headers, verify=False)
        assert res.ok, f"Error retrieving workflow columns\n{res.status_code}\n{res.text}"
        res_json = json.loads(res.text)
        df_list = []
        for col_type in res_json['data']:
            df_list.append(pd.json_normalize(res_json['data'][col_type]))
        return pd.concat(df_list)

    def task_count(self, wf_id: int) -> pd.DataFrame:
        """Creates a dataframe with the number of tasks in a workflow for each status and priority

        :param wf_id: Integer identifying the workflow
        :return: dataframe with count of tasks by status and priority
        """
        endpoint = f"{self.base_url}/api/v1/workflows/{wf_id}/taskstatuscount"
        res = requests.get(url=endpoint, headers=self.headers, verify=False)
        res_data = json.loads(res.text)
        df = pd.concat([pd.DataFrame(x, index=[0]) for x in res_data['data']['children']]).reset_index(drop=True)
        df['total_tasks'] = df[[col for col in df.columns if 'priority' in col.lower()]].sum(axis=1)
        logger.info(f"{df['total_tasks'].sum()} total tasks in workflow {wf_id}")
        return df

    def tasks(self, wf_id: int, page: int = 0, limit: int = 100, filters: list = None, columns: list = None) -> pd.DataFrame:
        """Returns up to 1MB of tasks as a dataframe (use bulk_export for larger number of tasks)

        :param wf_id: Integer identifying the workflow
        :param page: Use page and limit variables together, defaults to 0
        :param limit: Use page and limit variables together, defaults to 100
        :param filters: Tasks that do not meet these filters will not be returned
        :param columns: Returns only specified columns
        :return: dataframe of tasks
        """
        endpoint = f"{self.base_url}/api/v1/workflows/{wf_id}/tasks"
        parameters = {
            'wfId': wf_id,
            'page': page,
            'limit': limit
        }
        if filters:
            parameters['filters'] = '||'.join(filters)
        if columns:
            parameters['columns'] = '||'.join(columns)
        res = requests.get(url=endpoint, params=json.dumps(parameters), headers=self.headers, verify=False)
        assert res.ok, f"Error retrieving tasks\n{res.status_code}\n{res.text}"
        res_json = json.loads(res.text)
        return pd.json_normalize(res_json['data']['result'])

    def create_task(self, wf_id: int, task: dict, documents=None):
        """
        Used to create a single task

        :param wf_id: Integer identifying the workflow
        :param task: Dictionary of task parameters Example- {"PRIORITY":'High', "COUNTRY":"INDIA", "FSID":"100A", "COMMENT":"Task description"}
        :param documents: Documents are expected to be a list of dictionaries with id and filename as keys
        :return:
        """
        endpoint = f"{self.base_url}/api/v1/workflows/{wf_id}/tasks"
        task_request = {'task': task}
        if documents is not None:
            task_request['documents'] = documents
        res = requests.post(url=endpoint, data=json.dumps(task_request), headers=self.headers, verify=False)
        assert res.ok, f"Error creating task\n{res.status_code}\n{res.text}"
        # TODO: what should be returned here?
        return res

    def bulk_upload(self, wf_id: int, file):
        """There are 3 stages to the bulk upload process:
        1). get a presigned URL where the file will be uploaded
        2). upload the file to that presigned URL in binary
        3). check the result of the upload and download the results

        :param wf_id: integer identifying the workflow
        :param file: the absolute path of the CSV file to be uploaded
        :return:
        """
        endpoint = f"{self.base_url}/api/v1/workflows/{wf_id}/bulkfeed"
        file_path, file_name = os.path.split(file)

        def get_presigned_url():
            res = requests.post(url=endpoint, json={'file_name': file_name}, headers=self.headers, verify=False)
            assert res.ok, f"Error getting presigned URL\n{res.status_code}\n{res.text}"
            res_data = json.loads(res.text)
            csv_uuid = res_data['data']['csv_uuid']
            bulk_upload_url = res_data['data']['signed_url']['url']
            logger.info(f"csv_uuid: {csv_uuid}")
            logger.info(f"bulk_upload_url: {bulk_upload_url}")
            return csv_uuid, bulk_upload_url

        def upload_csv_file(bulk_upload_url, file):
            data = open(file, 'rb')
            upload_headers = {
                'Content-Type': 'application/binary'
            }
            res = requests.put(url=bulk_upload_url, data=data, headers=upload_headers)
            assert res.ok, f"Error when uploading file\n{res.status_code}\n{res.text}"

        def check_result(csv_uuid, process_name='BulkFeed'):
            file_link = ''
            is_timedout = True
            timedout_dict = {'true': True, 'false': False}
            attempt = 0
            max_retries = 5
            while is_timedout and attempt < max_retries:
                bulk_upload_file_response = self.files(file_id=csv_uuid, process_name=process_name)
                file_link = bulk_upload_file_response['data']['file_link']
                is_timedout = timedout_dict[bulk_upload_file_response['data']['is_timedout']]
                attempt += 1
                logger.info(f"Attempt {attempt}\n{bulk_upload_file_response}")
                time.sleep(10)
            if file_link != '':
                res = requests.get(url=file_link, verify=False)
                assert res.ok, f"Error when downloading file\n{res.status_code}\n{res.text}"
                df_res = pd.read_csv(StringIO(res.text))
                new_tasks = df_res.loc[df_res['message'].str.contains('created successfully'), 'task_id'].tolist()
                dupe_tasks = df_res.loc[df_res['message'].str.contains('already exists'), 'task_id'].tolist()
                logger.info(f"{len(new_tasks)} new tasks created: {','.join(new_tasks)}")
                logger.info(f"{len(dupe_tasks)} duplicate tasks: {','.join(dupe_tasks)}")

        csv_uuid, bulk_upload_url = get_presigned_url()
        upload_csv_file(bulk_upload_url, file)
        check_result(csv_uuid)

    def bulk_export(self, wf_id: int, action: str, from_datetime, **kwargs):
        endpoint = f"{self.base_url}/api/v1/workflows/{wf_id}/bulkexport"
        supported_actions = ('reports', 'getlistoftasks')
        if action not in supported_actions:
            raise ValueError(f"Unsupported action: {action}, supported actions {', '.join(supported_actions)}")
        parameters = {
            'wfId': wf_id,
            'action': action,
            'from_datetime': from_datetime
        }
        for k, v in kwargs.items():
            parameters[k] = v
        res = requests.get(url=endpoint, params=json.dumps(parameters), headers=self.headers, verify=False)

        # TODO: check the status of the export using files method, url should be in response
        return res


def main():
    wf_id = config['workflow']['id'].get()  # 15490
    gs = GenesysAPI(content_set='peopipe', user_id=21160, environment='stg')
    df_tasks_start = gs.task_count(wf_id)
    num_tasks_start = df_tasks_start['total_tasks'].sum()

    df = get_upcoming_ipos()
    wf_upload_file = os.path.join(os.getcwd(), 'temp_ipo_monitoring_upload.csv')
    df.to_csv(wf_upload_file, index=False)
    gs.bulk_upload(wf_id, wf_upload_file)

    df_tasks_end = gs.task_count(wf_id)
    num_tasks_end = df_tasks_end['total_tasks'].sum()
    logger.info(f"Difference of {num_tasks_end - num_tasks_start} tasks")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
        logger.error(e, exc_info=sys.exc_info())
