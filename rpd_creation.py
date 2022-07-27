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
from pg_connection import pg_connection, convert_cols_db, sql_types
from logging_ipo_dates import logger, error_email

pd.options.mode.chained_assignment = None


class RPDCreation:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('api_key.ini')
        self.source_file = os.path.join('Reference', 'IPO Monitoring Data.xlsx')
        self.result_file = os.path.join(os.getcwd(), 'Reference', 'IPO Monitoring RPDs.xlsx')
        self.wd_file = os.path.join(os.getcwd(), 'Reference', 'Withdrawn IPOs.xlsx')
        self.session = self.create_session()
        self.headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        self.base_url = 'https://rpd-api.factset.com/'
        self.df_ipo = self.return_formatted_df_from_file(self.source_file)
        self.df_wd = self.return_formatted_df_from_file(self.wd_file)
        self.df_rpd = self.rpd_data_frame()
        self.df = self.create_main_data_frame()
        self.rpd_cols = ['iconum', 'cusip', 'Company Name', 'ticker', 'exchange', 'IPO Date', 'Price', 'Price Range', 'status', 'notes', 'Last Checked', 'IPO Deal ID']

    @staticmethod
    def return_formatted_df_from_file(file: str):
        """
        Takes a file as input, creates a data frame from that file, does some clean-up and formatting.
        Used to create a data frame from IPO and withdrawn IPO files created in data_comparison.
        Data from sources takes precedence over internal data since it should be more up-to-date.

        :param file: os path of the file that will be turned into a dataframe
        :return:
        """
        df = pd.read_excel(file, dtype={'iconum': str, 'ticker': str},
                           parse_dates=['ipo_date', 'time_added', 'time_removed', 'trading_date', 'last_updated_date_utc'])
        df.rename(columns={'company_name_external': 'Company Name', 'price_external': 'Price', 'ipo_date': 'IPO Date',
                           'client_deal_id': 'IPO Deal ID', 'time_added': 'Last Checked', 'price_range': 'Price Range',
                           'exchange_external': 'exchange'
                           }, inplace=True)
        fillna_dict = {'Company Name': 'company_name_fds', 'IPO Date': 'trading_date',
                       'Price': 'price_fds', 'exchange': 'exchange_fds', 'Last Checked': 'last_updated_date_utc'}
        for col, fill_val in fillna_dict.items():
            df[col].fillna(df[fill_val], inplace=True)
        df = df[['iconum', 'cusip', 'Company Name', 'ticker', 'exchange', 'IPO Date', 'Price', 'Price Range',
                 'status', 'notes', 'Last Checked', 'IPO Deal ID']]
        df['formatted company name'] = df['Company Name'].str.lower()
        df['formatted company name'] = df['formatted company name'].str.replace(r"([\.\,\(\)\\\/])", "", regex=True)
        df['formatted company name'] = df['formatted company name'].str.replace(r"(\slimited$|\sltd|\ssa$|\sa/s$|\sinc$)", "", regex=True, case=False)
        df['formatted company name'] = df['formatted company name'].str.strip()
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

    def rpd_data_frame(self) -> pd.DataFrame:
        """
        Returns a data frame with the existing RPD information

        :return:
        """

        def user_created_rpds() -> pd.DataFrame:
            """
            Returns a data frame with the all RPDs created manually (i.e. not created by svc-ipo-monitoring)

            :return:
            """
            view_id = 31591815  # manually created IPO Monitoring RPDs
            view_endpoint = f'{self.base_url}view/{view_id}/results?page=1&pageSize=600'
            view_results = self.session.get(view_endpoint, headers=self.headers)
            view_json = view_results.json()
            view_data = []
            for row in view_json['Rows']:
                row_data = [c['Value'] for c in row['Cells']]
                view_data.append(row_data)
            df_view = pd.DataFrame(view_data)
            df_view.columns = [c['ColumnName'] for c in view_json['Columns']]
            df_view.rename(columns={
                'Message': 'RPD Number',
                'createdDate': 'RPD Creation Date',
                'answer_31405': 'IPO Date',
                'answer_31407': 'cusip',
                'answer_31406': 'exchange',
                'answer_31408': 'ticker',
                'Status': 'RPD Status'
            }, inplace=True)
            df_view['RPD Link'] = 'https://rpd.factset.com/summary.aspx?messageId=' + df_view['RPD Number'].astype(
                str)
            df_view['RPD Creation Date'] = pd.to_datetime(df_view['RPD Creation Date'], errors='coerce')
            df_view['RPD Creation Date'] = df_view['RPD Creation Date'].dt.tz_localize(None)
            # ticker in RPD sometimes has tabs or new line characters
            df_view['ticker'] = df_view['ticker'].str.extract(r"([\w\d\.]*)")
            # avoiding creating duplicate RPDs
            # if an RPD has either the same ticker or cusip, do not create a new RPD
            df_view = pd.concat((
                df_view[['ticker', 'RPD Number', 'RPD Link', 'RPD Creation Date', 'RPD Status']].merge(self.df_ipo, on='ticker'),
                df_view[['cusip', 'RPD Number', 'RPD Link', 'RPD Creation Date', 'RPD Status']].merge(self.df_ipo, on='cusip'),
            ))
            logger.info(f'Matched {len(df_view)} manually created RPDs to IPOs.')
            return df_view
        df_rpd = pd.read_excel(self.result_file, dtype={'iconum': str})
        df_user_created_rpds = user_created_rpds()
        df_rpd = pd.concat([df_rpd, df_user_created_rpds])
        df_rpd.sort_values(by=['RPD Status', 'RPD Creation Date'], na_position='first', inplace=True)
        return df_rpd

    def create_main_data_frame(self):
        """
        Creates the main data frame that has IPO and RPD information.
        Here the data is concatenated and rows without RPD information are dropped if they have the same formatted name.

        :return:
        """
        df = pd.concat([self.df_ipo, self.df_rpd], ignore_index=True)
        df.sort_values(by=['RPD Creation Date'], inplace=True)
        df.drop_duplicates(subset='formatted company name', inplace=True, ignore_index=True)
        return df

    def get_rpd_status(self, rpd_num: int) -> str:
        """
        Return the status of the RPD number provided

        :param rpd_num: the number identifying the RPD
        :return: RPD status as text
        """
        status_endpoint = self.base_url + f"rpd/{rpd_num}/Status"
        res_status = self.session.get(url=status_endpoint, headers=self.headers)
        if res_status.ok:
            return res_status.text.replace('"', '')

    def get_rpd_resolution(self, rpd_num: int) -> json:
        """
        Returns the resolution for resolved RPDs
        If the RPD has not been resolved, the API will return 'null'

        :param rpd_num: the number identifying the RPD
        :return:
        """
        resolution_endpoint = self.base_url + f"rpd/{rpd_num}/Resolution"
        res_resolution = self.session.get(url=resolution_endpoint, headers=self.headers)
        if res_resolution.ok and res_resolution.text != 'null':
            return json.loads(res_resolution.text)

    def update_withdrawn_ipos(self):
        """
        If an IPO is withdrawn, the RPD will be updated with a comment showing that the status is withdrawn
        and in the main data frame the RPD Status will be set to Resolved (so that I no longer update the RPD).

        :return:
        """
        df_wd = pd.merge(self.df_wd, self.df_rpd, how='inner', on='formatted company name', suffixes=('', '_'))
        df_wd = df_wd.loc[df_wd['RPD Status'] != 'Resolved']
        if len(df_wd) > 0:
            df_wd['IPO Date'] = df_wd['IPO Date'].dt.strftime('%Y-%m-%d')
            logger.info(f"{len(df_wd)} RPDs to update for withdrawn IPOs: {', '.join([str(int(num)) for num in df_wd['RPD Number'].to_list()])}")
            df_wd.replace(np.nan, '', inplace=True)
            for idx, row in df_wd.iterrows():
                rpd = int(row['RPD Number'])
                ipo_html = row.loc[self.rpd_cols].to_frame().to_html(header=False, na_rep='', justify='left')
                comment_endpoint = self.base_url + f'rpd/{int(rpd)}/comments'
                rpd_comment = {'Content': ipo_html}
                res_c = self.session.post(comment_endpoint, data=json.dumps(rpd_comment), headers=self.headers)
                self.df.loc[self.df['RPD Number'] == rpd, 'Status'] = 'Withdrawn'
                self.df.loc[self.df['RPD Number'] == rpd, 'RPD Status'] = 'Resolved'

    def update_rpds(self):
        """
        Updating existing RPDs when either the IPO Date, CUSIP or ticker have changed.
        The data is merged so that I can see what data has changed (if any).

        :return:
        """
        df_rpd = pd.merge(self.df_ipo, self.df_rpd, how='left', on='formatted company name', suffixes=('', '_old'))
        df_rpd = df_rpd.loc[
            (df_rpd['RPD Number'].notna())
            & (df_rpd['RPD Status'] != 'Resolved')
        ]
        # only make one update per RPD using the latest information
        df_rpd.sort_values(by=['Last Checked'], ascending=False, inplace=True)
        df_rpd.drop_duplicates(subset='RPD Number', inplace=True, ignore_index=True)
        # compare the data
        df_rpd['IPO Date Comparison'] = df_rpd['IPO Date'] == df_rpd['IPO Date_old']
        df_rpd['exchange Comparison'] = df_rpd['exchange'] == df_rpd['exchange_old']
        df_rpd['cusip Comparison'] = df_rpd['cusip'] == df_rpd['cusip_old']
        df_rpd.loc[df_rpd['cusip'].isna(), 'cusip Comparison'] = True
        df_rpd['ticker Comparison'] = df_rpd['ticker'] == df_rpd['ticker_old']
        df_rpd.loc[df_rpd['ticker'].isna(), 'ticker Comparison'] = True
        # filter for only updated data (i.e. where comparison is False)
        df_rpd = df_rpd.loc[
            (~df_rpd['IPO Date Comparison'])
            | (~df_rpd['cusip Comparison'])
            | (~df_rpd['ticker Comparison'])
            # | (~df_rpd['exchange Comparison'])
        ]
        # update the main data frame with updated data to prevent from making the same comment multiple times
        # note this will only update when IPO date, CUSIP or ticker have changed. Other changes won't be added.
        df_rpd = df_rpd[['iconum', 'cusip', 'Company Name', 'ticker', 'exchange', 'IPO Date', 'Price', 'Price Range',
                         'status', 'notes', 'Last Checked', 'IPO Deal ID', 'formatted company name', 'RPD Number',
                         'RPD Link', 'RPD Creation Date', 'RPD Status']]
        self.df = pd.concat([self.df, df_rpd], ignore_index=True).drop_duplicates(subset=['RPD Number', 'formatted company name'], keep='last')
        logger.info(f"{len(df_rpd)} updates to make on existing RPDs: {', '.join([str(int(num)) for num in df_rpd['RPD Number'].to_list()])}")
        df_rpd['IPO Date'] = df_rpd['IPO Date'].dt.strftime('%Y-%m-%d')
        df_rpd.replace(np.nan, '', inplace=True)
        for idx, row in df_rpd.iterrows():
            rpd = int(row['RPD Number'])
            rpd_status = self.get_rpd_status(rpd)
            # update the main data frame with the status
            self.df.loc[self.df['RPD Number'] == rpd, 'RPD Status'] = rpd_status
            if rpd_status == 'Resolved':
                rpd_resolution = self.get_rpd_resolution(rpd)
                dupe_rpd = rpd_resolution.get('DuplicateRPD')
                if dupe_rpd:
                    # if RPD is resolved and not a duplicate dupe_rpd will be None
                    # if this RPD was resolved as a duplicate RPD, update the main data frame with the new RPD number
                    # not updating comments of the dupe RPD since that should already be done in the row for that RPD
                    self.df.loc[self.df['RPD Number'] == rpd, 'RPD Link'] = 'https://rpd.factset.com/summary.aspx?messageId=' + str(dupe_rpd)
                    self.df.loc[self.df['RPD Number'] == rpd, 'RPD Number'] = dupe_rpd
                    self.df.loc[self.df['RPD Number'] == rpd, 'RPD Status'] = ''
            else:
                # only adding comments to RPDs that have not been resolved (will still add comments to completed RPDs)
                fds_cusip = str(row['cusip'])
                ipo_date = str(row['IPO Date'])
                ticker = str(row['ticker'])
                exchange = str(row['exchange'])
                ipo_html = row.loc[self.rpd_cols].to_frame().to_html(header=False, na_rep='', justify='left')
                comment_endpoint = self.base_url + f'rpd/{int(rpd)}/comments'
                rpd_comment = {'Content': ipo_html}
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

    def create_new_rpds(self) -> dict:
        """
        Creates new RPDs for all the IPOs that currently do not have an RPD.

        :return: Dictionary with data about the new RPDs created
        """
        endpoint = self.base_url + 'rpd'
        rpd_dict = defaultdict(list)
        df_rpd = self.df.copy()
        # filtering for only IPOs that do not have an RPD Number
        df_rpd = df_rpd.loc[df_rpd['RPD Number'].isna()]
        df_rpd['IPO Date'] = df_rpd['IPO Date'].dt.strftime('%Y-%m-%d')
        df_rpd.replace(np.nan, '', inplace=True)
        for idx, row in df_rpd.iterrows():
            company_name = str(row['Company Name'])
            exchange = str(row['exchange'])
            fds_cusip = str(row['cusip'])
            ipo_date = str(row['IPO Date'])
            ticker = str(row['ticker'])
            ipo_html = row.loc[self.rpd_cols].to_frame().to_html(header=False, na_rep='', justify='left')
            rpd_request = {
                'Title': f"{company_name} - {exchange}",
                'Products': [{'Id': '106317'}],
                'Content': ipo_html,
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
            if company_name is not None and company_name != '':
                res = self.session.post(url=endpoint, data=json.dumps(rpd_request), headers=self.headers)
                if res.ok:
                    rpd_num = res.headers['X-IS-ID']
                    rpd_date = res.headers['Date']
                    # rpd_api_link = res.headers['Location']
                    rpd_dict['Company Name'].append(company_name)
                    rpd_dict['RPD Number'].append(rpd_num)
                    rpd_dict['RPD Link'].append('https://rpd.factset.com/summary.aspx?messageId=' + str(rpd_num))
                    rpd_dict['RPD Creation Date'].append(rpd_date)
                    rpd_dict['RPD Status'].append('Pending')
            sleep(1)
        logger.info(f"Created {len(rpd_dict['RPD Number'])} new RPDs: {', '.join([str(num) for num in rpd_dict['RPD Number']])}")
        return rpd_dict

    def add_new_rpds(self):
        """
        Creates a data frame from the dictionary returned by create_new_rpds and adds the new RPD information to
        the main data frame.

        :return:
        """
        rpd_dict = self.create_new_rpds()
        if rpd_dict is not None and len(rpd_dict['RPD Number']) > 0:
            df_rpd = pd.DataFrame(rpd_dict)
            df_rpd['RPD Creation Date'] = pd.to_datetime(df_rpd['RPD Creation Date'].fillna(pd.NaT), errors='coerce').dt.tz_localize(None)
            # adding new data to main data frame
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
                           'cusip',
                           'Company Name',
                           'ticker',
                           'exchange',
                           'IPO Date',
                           'Price',
                           'Price Range',
                           'status',
                           'notes',
                           'Last Checked',
                           'IPO Deal ID',
                           'formatted company name',
                           'RPD Number',
                           'RPD Link',
                           'RPD Creation Date',
                           'RPD Status']]
        self.df.to_excel(self.result_file, index=False, encoding='utf-8-sig')
        conn = pg_connection()
        try:
            self.df.columns = convert_cols_db(self.df.columns)
            self.df.to_sql('rpd_ipo_monitoring', conn, if_exists='replace', index=False,
                           dtype={
                               'iconum': sql_types.INT,
                               'ipo_date': sql_types.Date,
                               'last_checked': sql_types.DateTime,
                               'rpd_number': sql_types.INT,
                               'rpd_creation_date': sql_types.DateTime
                           })
        except Exception as e:
            logger.error(e)
        finally:
            conn.close()


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
        rpd.update_withdrawn_ipos()
        rpd.update_rpds()
        rpd.add_new_rpds()
        rpd.save_results()
    except Exception as e:
        logger.error(e, exc_info=sys.exc_info())
        error_email(str(e))


if __name__ == '__main__':
    main()
