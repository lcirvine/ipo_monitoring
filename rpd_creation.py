import os
import sys
import pandas as pd
import numpy as np
import configparser
import requests
from requests_ntlm import HttpNtlmAuth
import json
from time import sleep
from collections import defaultdict
from logging_ipo_dates import logger, error_email

pd.options.mode.chained_assignment = None


class RPDCreation:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('api_key.ini')
        self.result_file = os.path.join(os.getcwd(), 'Reference', 'IPO Monitoring RPDs.xlsx')
        self.df = self.create_data_frame()
        self.session = self.create_session()
        self.headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        self.base_url = 'http://is.factset.com/rpd/api/v2/'
        self.endpoint = self.base_url + 'rpd'

    def create_data_frame(self) -> pd.DataFrame:
        """
        Creates the main data frame that will be used to create and update RPDs.
        Currently only creating/updating RPDs for IPOs where the IPO Date is >= today.
        :return: Data frame with all the information that will go into the RPD. Existing information that was previously
        sent in the RPD will have _old appended to the column name.
        """
        df = pd.read_excel(os.path.join('Reference', 'IPO Monitoring Data.xlsx'),
                           dtype={'iconum': str, 'Symbol': str, 'ticker': str},
                           converters={'IPO Date': pd.to_datetime,
                                       'time_checked': pd.to_datetime,
                                       'trading_date': pd.to_datetime,
                                       'last_updated_date_utc': pd.to_datetime})
        df.rename(columns={'Company Name_external': 'Company Name', 'Price_external': 'Price',
                           'client_deal_id': 'IPO Deal ID', 'time_checked': 'Last Checked'}, inplace=True)
        fillna_dict = {'Company Name': 'Company Name_fds', 'IPO Date': 'trading_date', 'Price': 'Price_fds',
                       'Symbol': 'ticker', 'Market': 'exchange', 'Last Checked': 'last_updated_date_utc'}
        for col, fill_val in fillna_dict.items():
            df[col].fillna(df[fill_val], inplace=True)
        df = df[['iconum', 'CUSIP', 'Company Name', 'Symbol', 'Market', 'IPO Date', 'Price', 'Price Range',
                 'Status', 'Notes', 'Last Checked', 'IPO Deal ID']]
        if os.path.exists(self.result_file):
            df = pd.merge(df, pd.read_excel(self.result_file), how='left', on='Company Name', suffixes=('', '_old'))
        return df

    def create_session(self) -> requests.Session:
        """
        Creates a session using the uername and password provided in the config file.
        :return: A session that will be used in RPDCreation
        """
        uname = self.config.get('credentials', 'username')
        pword = self.config.get('credentials', 'password')
        session = requests.Session()
        session.auth = HttpNtlmAuth(uname, pword, session)
        return session
    
    def update_rpds(self, separator: str = ':  '):
        """
        Updating existing RPDs when either the IPO Date or CUSIP has changed.
        Previous information like the IPO Date and CUSIP that were originally entered into the RPD have _old appended
        to the column. The _old columns will not be saved so there is no need to update the main data frame.
        :param separator: A separator is added to the columns to make the RPD comment more readable.
        :return:
        """
        df_rpd = self.df.copy()
        df_rpd = df_rpd.loc[df_rpd['RPD Number'].notna()]
        df_rpd['IPO Date Comparison'] = df_rpd['IPO Date'] == df_rpd['IPO Date_old']
        df_rpd['Market Comparison'] = df_rpd['Market'] == df_rpd['Market_old']
        df_rpd['CUSIP Comparison'] = df_rpd['CUSIP'] == df_rpd['CUSIP_old']
        df_rpd.loc[df_rpd['CUSIP'].isna(), 'CUSIP Comparison'] = True
        df_rpd['Symbol Comparison'] = df_rpd['Symbol'] == df_rpd['Symbol_old']
        df_rpd.loc[df_rpd['Symbol'].isna(), 'Symbol Comparison'] = True
        df_rpd = df_rpd.loc[
            (~df_rpd['IPO Date Comparison'])
            | (~df_rpd['CUSIP Comparison'])
            | (~df_rpd['Symbol Comparison'])
            | (~df_rpd['Market Comparison'])
        ]
        df_rpd['IPO Date'] = df_rpd['IPO Date'].dt.strftime('%Y-%m-%d')
        df_rpd['Last Checked'] = df_rpd['Last Checked'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_rpd = df_rpd[['iconum', 'CUSIP', 'Company Name', 'Symbol', 'Market', 'IPO Date', 'Price', 'Price Range', 
                         'Status', 'Notes', 'Last Checked', 'IPO Deal ID', 'RPD Number', 'RPD Link',
                         'RPD Creation Date']]
        logger.info(f"{len(df_rpd)} updates to make on existing RPDs: {', '.join(df_rpd['RPD Number'].to_list())}")
        df_rpd.replace(np.nan, '', inplace=True)
        # adding separator to columns to make it more readable in the RPD
        df_rpd.columns = [col + separator for col in df_rpd.columns]
        for idx, row in df_rpd.iterrows():
            rpd = int(row['RPD Number' + separator])
            fds_cusip = str(row['CUSIP' + separator])
            ipo_date = str(row['IPO Date' + separator])
            ticker = str(row['Symbol' + separator])
            exchange = str(row['Market' + separator])
            ipo_string = df_rpd.loc[idx].drop([c for c in df_rpd.columns if 'RPD' in c]).to_string(na_rep='').replace('\n', '<br>')
            comment_endpoint = self.base_url + f'rpd/{int(rpd)}/comments'
            rpd_comment = {'Content': ipo_string}
            res_c = self.session.post(comment_endpoint, data=json.dumps(rpd_comment), headers=self.headers)

            question_endpoint = self.base_url + f'rpd/{int(rpd)}/questions'
            questions = [
                    {
                        'Id': 31407,
                        'Answers': [{'AnswerValue': fds_cusip}]
                    },
                    {
                        'Id': 31405,
                        'Answers': [{'AnswerValue': ipo_date}]
                    },
                    {
                        'Id': 31406,
                        'Answers': [{'AnswerValue': exchange}]
                    },
                    {
                        'Id': 31408,
                        'Answers': [{'AnswerValue': ticker}]
                    }
                ]
            res_q = self.session.post(question_endpoint, data=json.dumps(questions), headers=self.headers)

    def create_new_rpds(self, separator: str = ':  ') -> dict:
        """
        Creates new RPDs for all the IPOs that currently do not have an RPD.
        :param separator: A separator is added to the columns to make the RPD comment more readable.
        :return: Dictionary with data about the new RPDs created
        """
        rpd_dict = defaultdict(list)
        df_rpd = self.df.copy()
        df_rpd = df_rpd.loc[df_rpd['RPD Number'].isna()]
        df_rpd['IPO Date'] = df_rpd['IPO Date'].dt.strftime('%Y-%m-%d')
        df_rpd['Last Checked'] = df_rpd['Last Checked'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_rpd = df_rpd[['iconum', 'CUSIP', 'Company Name', 'Symbol', 'Market', 'IPO Date', 'Price', 'Price Range',
                         'Status', 'Notes', 'Last Checked', 'IPO Deal ID']]
        df_rpd.replace(np.nan, '', inplace=True)
        df_rpd.columns = [col + separator for col in df_rpd.columns]
        for idx, row in df_rpd.iterrows():
            ipo_string = df_rpd.loc[idx].to_string(na_rep='').replace('\n', '<br>')
            company_name = str(row['Company Name' + separator])
            exchange = str(row['Market' + separator])
            fds_cusip = str(row['CUSIP' + separator])
            ipo_date = str(row['IPO Date' + separator])
            ticker = str(row['Symbol' + separator])
            rpd_request = {
                'Title': f"{company_name} - {exchange}",
                'Products': [{'Id': '106317'}],
                'Content': ipo_string,
                'Type': 'EnhancementRequest',
                'Priority': 'Medium',
                'Severity': 'Medium',
                'Questions': [
                    {
                        'Id': 31407,
                        'Answers': [{'AnswerValue': fds_cusip}]
                    },
                    {
                        'Id': 31405,
                        'Answers': [{'AnswerValue': ipo_date}]
                    },
                    {
                        'Id': 31406,
                        'Answers': [{'AnswerValue': exchange}]
                    },
                    {
                        'Id': 31408,
                        'Answers': [{'AnswerValue': ticker}]
                    }
                ]
            }
            res = self.session.post(url=self.endpoint, data=json.dumps(rpd_request), headers=self.headers)
            if res.ok:
                rpd_num = res.headers['X-IS-ID']
                rpd_date = res.headers['Date']
                # rpd_api_link = res.headers['Location']
                rpd_dict['Company Name'].append(company_name)
                rpd_dict['RPD Number'].append(rpd_num)
                rpd_dict['RPD Link'].append('https://is.factset.com/rpd/summary.aspx?messageId=' + str(rpd_num))
                rpd_dict['RPD Creation Date'].append(rpd_date)
            sleep(1)
        logger.info(f"Created {len(rpd_dict)} new RPDs: {', '.join([str(num) for num in rpd_dict['RPD Number']])}")
        return rpd_dict

    def add_new_rpds(self):
        """
        Creates a data frame from the dictionary returned by create_new_rpds and adds the new RPD information to
        the main data frame.
        :return:
        """
        rpd_dict = self.create_new_rpds()
        if rpd_dict is not None and len(rpd_dict) > 0:
            df_rpd = pd.DataFrame(rpd_dict)
            df_rpd['RPD Link'] = 'https://is.factset.com/rpd/summary.aspx?messageId=' + df_rpd['RPD Number'].astype(str)
            df_rpd['RPD Creation Date'] = pd.to_datetime(df_rpd['RPD Creation Date'].fillna(pd.NaT), errors='coerce').dt.tz_localize(None)
            self.df = pd.merge(self.df, df_rpd, how='left', on='Company Name', suffixes=('', '_new'))
            fillna_dict = {
                'RPD Number': 'RPD Number_new',
                'RPD Link': 'RPD Link_new',
                'RPD Creation Date': 'RPD Creation Date_new'}
            for col, fill_val in fillna_dict.items():
                self.df[col].fillna(self.df[fill_val], inplace=True)
        
    def save_results(self):
        """
        Closes the session and saves the results of the updated data frame.
        :return:
        """
        self.session.close()
        self.df = self.df[['iconum',
                           'CUSIP',
                           'Company Name',
                           'Symbol',
                           'Market',
                           'IPO Date',
                           'Price',
                           'Price Range',
                           'Status',
                           'Notes',
                           'Last Checked',
                           'IPO Deal ID',
                           'RPD Number',
                           'RPD Link',
                           'RPD Creation Date']]
        self.df.to_excel(self.result_file, index=False, encoding='utf-8-sig')


def resolve_rpds():
    """
    Used to resolve RPDs in bulk.
    :return:
    """
    config = configparser.ConfigParser()
    config.read('api_key.ini')
    session = requests.Session()
    session.auth = HttpNtlmAuth(config.get('credentials', 'username'), config.get('credentials', 'password'), session)
    result_file = os.path.join(os.getcwd(), 'Reference', 'IPO Monitoring RPDs.xlsx')
    base_url = 'http://is.factset.com/rpd/api/v2/'
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

    df_rpd = pd.read_excel(result_file)
    for rpd in df_rpd['RPD Number'].tolist():
        resolve_endpoint = base_url + f'/rpd/{int(rpd)}/comments'
        rpd_comment = {
            'Content': 'Resolving RPD.',
            'Status': 'Resolved',
            'Resolution': {
                'Code': 10,
                'Description': 'Clarified Behavior/Answered Question'
            },
            'SendOption': 0
        }
        res = session.post(resolve_endpoint, data=json.dumps(rpd_comment), headers=headers)


def main():
    try:
        rpd = RPDCreation()
        rpd.update_rpds()
        rpd.add_new_rpds()
        rpd.save_results()
    except Exception as e:
        logger.error(e, exc_info=sys.exc_info())
        error_email(str(e))


if __name__ == '__main__':
    main()
