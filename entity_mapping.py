import os
import sys
import pandas as pd
import numpy as np
import json
from datetime import datetime
from time import sleep
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from configparser import ConfigParser
from logging_ipo_dates import logger, error_email, log_folder

pd.options.mode.chained_assignment = None
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class EntityMatchBulk:
    def __init__(self):
        self.config = ConfigParser()
        self.config.read('api_key.ini')
        self.ref_folder = os.path.join(os.getcwd(), 'Reference')
        self.entity_mapping_file = os.path.join(self.ref_folder, 'Entity Mapping.xlsx')
        self.file_name = f'upcoming_IPO_entity_mapping_{datetime.utcnow().strftime("%Y-%m-%d %H%M")}'
        self.file = os.path.join(self.ref_folder, 'Entity Mapping Requests', self.file_name + '.csv')
        self.authorization = (self.config.get('FDSAPI', 'USERNAME-SERIAL'), self.config.get('FDSAPI', 'API-Key'))
        self.headers = {'Accept': 'application/json;charset=utf-8'}

    def create_csv(self, recheck_all: bool = False):
        # create a dataframe of all company names without iconums including new names found
        df_e = pd.read_excel(self.entity_mapping_file, usecols=['Company Name', 'iconum', 'entity_id', 'mapStatus'])
        df_s = pd.read_excel(os.path.join(os.getcwd(), 'Results', 'All IPOs.xlsx'),
                             usecols=['Company Name', 'Symbol', 'Market'])
        df = pd.merge(df_s, df_e, how='outer', on='Company Name')
        if recheck_all:
            # checking 1. company names that aren't null 2. don't have an iconum
            df = df.loc[~df['Company Name'].isna() & df['iconum'].isna()]
        else:
            # only checking 1. company names that aren't null 2. don't have an iconum and 3. haven't been checked yet
            df = df.loc[~df['Company Name'].isna() & df['iconum'].isna() & df['mapStatus'].isna()]
        df = df.drop_duplicates()
        logger.info(f"{len(df)} unmapped entities")
        # making unique client_id by concatenating company name, symbol and market separated by underscores
        df['client_id'] = df['Company Name'].fillna('') + '_' + df['Symbol'].fillna('').astype(str) + '_' + df['Market'].fillna('')
        df.set_index('client_id', inplace=True)
        # save that dataframe to a csv encoded as utf8
        if len(df) > 1:
            df.to_csv(self.file, index_label='client_id', encoding='utf-8-sig')

    def entity_mapping_api(self):
        if os.path.exists(self.file):
            # create request with concordance API
            entity_task_endpoint = 'https://api.factset.com/content/factset-concordance/v1/entity-task'
            entity_task_request = {
                'taskName': self.file_name,
                'clientIdColumn': 'client_id',
                'nameColumn': 'Company Name',
                'includeEntityType': ['PUB', 'PVT', 'HOL', 'SUB'],
                'uniqueMatch': True
            }
            with open(self.file, 'rb') as f:
                file_data = {'inputFile': (self.file_name + '.csv', f, 'text/csv')}
                entity_task_response = requests.post(url=entity_task_endpoint, data=entity_task_request,
                                                     auth=self.authorization, files=file_data, headers=self.headers)
            assert entity_task_response.ok, f"{entity_task_response.status_code} - {entity_task_response.text}"
            # temporarily saving entity task response to look into errors
            # getting Bad Request - Number of elements in the header doesn't match the total number of columns
            with open(os.path.join(log_folder, 'Concordance API Responses', f"API response for {self.file_name}.txt"), 'w', encoding='utf8') as f:
                json.dump(entity_task_response.text, f, ensure_ascii=False)
            if entity_task_response.text is not None and entity_task_response.text != '':
                entity_task_data = json.loads(entity_task_response.text)
                eid = entity_task_data['data']['taskId']
                task_name = entity_task_data['data']['taskName']  # will be file_name provided in entity task request
                logger.info(f"Entity mapping request submitted - task ID {eid} - task name {task_name}")
                task_status = self.get_task_status(eid)
                logger.info(f"Task {eid} status - {task_status}")
                if task_status == 'SUCCESS':
                    df_result = self.get_entity_decisions(eid)
                    self.formatting_and_saving(df_result)
        else:
            logger.info(f"File not found - {self.file}")

    def get_task_status(self, eid, recheck_count: int = 0, max_recheck=12, wait_time=10):
        # get the status of the request
        entity_task_status_endpoint = 'https://api.factset.com/content/factset-concordance/v1/entity-task-status'
        status_parameters = {
            'taskId': str(eid)
        }
        entity_task_status_response = requests.get(url=entity_task_status_endpoint, params=status_parameters,
                                                   auth=self.authorization, headers=self.headers, verify=False)
        entity_task_status_data = json.loads(entity_task_status_response.text)
        task_status = entity_task_status_data['data'][0]['status']
        if task_status in ['PENDING', 'IN-PROGRESS'] and recheck_count < max_recheck:
            recheck_count += 1
            sleep(wait_time)
            return self.get_task_status(eid, recheck_count)
        else:
            logger.info(f"Duration for Concordance API {entity_task_status_data['data'][0]['processDuration']}")
            logger.info(f"Decision Rate for Concordance API {entity_task_status_data['data'][0]['decisionRate']}")
            return task_status

    def get_entity_decisions(self, eid):
        # get the entity mappings returned from the API
        # seems that the default limit for number of results returned is 100 so increasing to 1000
        entity_decisions_endpoint = 'https://api.factset.com/content/factset-concordance/v1/entity-decisions'
        decisions_parameters = {
            'taskId': str(eid),
            'limit': 1000
        }
        entity_decisions_response = requests.get(url=entity_decisions_endpoint, params=decisions_parameters,
                                                 auth=self.authorization, headers=self.headers, verify=False)
        entity_decisions_data = json.loads(entity_decisions_response.text)
        return pd.json_normalize(entity_decisions_data['data'])
        
    def formatting_and_saving(self, df: pd.DataFrame):
        # save results from Concordance API
        # braking out the client_id back into company name, Symbol and Market columns
        df[['company_name', 'Symbol', 'Market']] = df['clientId'].str.split('_', expand=True)
        df.sort_values(by=['confidenceScore', 'similarityScore'], ascending=False, inplace=True)
        df.replace('', np.nan, inplace=True)
        df['iconum'] = df['entityId'].map(self.entity_id_to_iconum, na_action='ignore')
        cols = ['clientName', 'entityName', 'iconum', 'entityId', 'matchFlag', 'mapStatus', 'similarityScore',
                'confidenceScore', 'countryCode', 'countryName', 'entityTypeCode', 'entityTypeDescription',
                'nameMatchString', 'nameMatchSource', 'taskId', 'rowIndex', 'clientId', 'company_name', 'Symbol',
                'Market', 'clientCountry', 'clientState', 'clientUrl']
        df = df[cols]
        df.to_csv(os.path.join(self.ref_folder, 'Entity Mapping Requests', self.file_name + '_results.csv'),
                  index=False, encoding='utf-8-sig')
        df.drop_duplicates(subset=['clientName'], inplace=True)
        # add results to entity mapping file
        df.rename(columns={'clientName': 'Company Name', 'entityId': 'entity_id'}, inplace=True)
        final_cols = ['Company Name', 'Symbol', 'Market', 'entityName', 'iconum', 'entity_id', 'mapStatus',
                      'similarityScore', 'confidenceScore', 'countryName', 'entityTypeDescription']
        df = df[final_cols]
        # rows with confidence below .75 should be not be considered matches
        df.loc[df['confidenceScore'] <= .75, 'mapStatus'] = 'UNMAPPED'
        df.loc[df['confidenceScore'] <= .75, ['entityName', 'iconum', 'entity_id', 'similarityScore', 'confidenceScore',
                                              'countryName', 'entityTypeCode', 'entityTypeDescription']] = np.nan
        if os.path.exists(self.entity_mapping_file):
            df = pd.concat([df, pd.read_excel(self.entity_mapping_file)], ignore_index=True)
        df.sort_values(by=['entityName', 'Company Name'], inplace=True)
        df.drop_duplicates(subset=['Company Name'], inplace=True)
        df.to_excel(self.entity_mapping_file, index=False, encoding='utf-8-sig')

    @staticmethod
    def iconum_to_entity_id(iconum: int):
        chars = "0123456789BCDFGHJKLMNPQRSTVWXYZ"
        temp_ent = iconum
        ent_id = ''
        for x in range(5, -1, -1):
            p = len(chars) ** x
            char_index = temp_ent // p
            ent_id += chars[char_index]
            temp_ent = temp_ent % p
        return ent_id + '-E'

    @staticmethod
    def entity_id_to_iconum(entity_id: str):
        chars = "0123456789BCDFGHJKLMNPQRSTVWXYZ"
        temp_iconum = 0
        for x in range(0, 6):
            ss = entity_id[x]
            char_index = chars.find(ss)
            p = len(chars) ** (5 - x)
            temp_iconum += p * char_index
        return temp_iconum


def main():
    logger.info("Checking Cordance API for entity IDs")
    em = EntityMatchBulk()
    try:
        em.create_csv(recheck_all=True)
        em.entity_mapping_api()
    except Exception as e:
        logger.error(e, exc_info=sys.exc_info())
        logger.info('-' * 100)
        error_email(str(e))


if __name__ == '__main__':
    main()
