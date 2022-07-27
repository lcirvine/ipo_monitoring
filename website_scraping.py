import os
import sys
import csv
import json
import time
from random import randint
from datetime import datetime
from logging_ipo_dates import logger, log_folder
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import pandas as pd
import numpy as np
import requests
from pg_connection import pg_connection, sql_types
from collections import defaultdict


class WebDriver:
    def __init__(self, headless: bool = True):
        opts = Options()
        if headless:
            opts.headless = True
        self.driver = webdriver.Firefox(options=opts)
        self.sleep_time = 5
        self.time_checked = datetime.utcnow()
        self.time_checked_str = self.time_checked.strftime('%Y-%m-%d %H:%M')
        self.source_data_folder = os.path.join(os.getcwd(), 'Data from Sources')
        if not os.path.exists(self.source_data_folder):
            os.mkdir(self.source_data_folder)
        sources_file = os.path.join(os.getcwd(), 'sources.json')
        if os.path.exists(sources_file):
            with open(sources_file, 'r') as f:
                self.sources = json.load(f)
            self.website_sources = {k: v for k, v in self.sources.items() if v['source_type'] == 'website'}
        self.webscraping_results = []
        self.conn = pg_connection()

    @staticmethod
    def random_wait(max_wait_sec: int = 120):
        wait_time = randint(0, max_wait_sec)
        time.sleep(wait_time)

    def load_url(self, url: str, sleep_after: bool = False):
        """
        Loads the URL provided as a parameter and optionally waits after loading the page
        :param url: The URL to be loaded in the driver
        :param sleep_after: Bool for waiting after loading the URL
        :return: None
        """
        assert url is not None, f'No URL given'
        if url != self.driver.current_url:
            self.driver.get(url)
            if sleep_after:
                time.sleep(self.sleep_time)

    def return_soup(self) -> BeautifulSoup:
        """
        Returns parsed HTML from driver as a BeautifulSoup object
        :return: BeautifulSoup object
        """
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        return soup

    def parse_table(self, get_links: bool = False, **kwargs) -> pd.DataFrame:
        """
        Parses the element identified by the keyword arguments and returns a pandas dataframe
        :param get_links: bool, if true the function will add links to data returned
        :return: pandas dataframe
        """
        url = kwargs.get('url')
        table_elem = kwargs.get('table_elem')
        table_num = kwargs.get('table_num', 0)
        table_attrs = kwargs.get('table_attrs')
        table_title = kwargs.get('table_title')
        row_elem = kwargs.get('row_elem')
        cell_elem = kwargs.get('cell_elem')
        header_elem = kwargs.get('header_elem')
        link_elem = kwargs.get('link_elem')
        link_key = kwargs.get('link_key')
        cols = kwargs.get('columns')
        column_names_as_row = kwargs.get('column_names_as_row')

        soup = self.return_soup()
        if table_title is not None and soup.find(text=table_title) is not None:
            table = soup.find(text=table_title).parent.parent.find(table_elem)
        elif table_attrs is None:
            table = soup.find_all(table_elem)[table_num]
        else:
            table = soup.find(table_elem, attrs=table_attrs)
        assert table is not None, f'Unable to find {table_elem} with these attributes {table_attrs} on {url}'
        table_data = []
        for row in table.find_all(row_elem):
            cells = [c.text.strip() for c in row.find_all(cell_elem)]
            if get_links and link_elem is not None and link_key is not None:
                for link in row.find_all(link_elem):
                    cells.append(link[link_key])
            if len(cells) > 1 and (cells[1] != cols[1]):
                table_data.append(cells)
        df = pd.DataFrame(table_data)
        if len(df) > 0:
            # adding columns for dataframe and making sure the column list is the correct length
            cols_in_row = len(df.loc[0])
            if len(cols) < cols_in_row:
                cols.extend([f"Unnamed_column_{c}" for c in range(cols_in_row - len(cols))])
            elif len(cols) > cols_in_row:
                cols = cols[0:cols_in_row]
            df.columns = cols
            df = df.replace(r'^\s*$', np.nan, regex=True)
            df.dropna(how='all', inplace=True)
            # Some sources give the column headers as rows in the table
            if column_names_as_row:
                df = df.drop(0).reset_index(drop=True)
            df['time_checked'] = self.time_checked_str
            return df

    def special_cases(self):

        def asx():
            try:
                url = self.sources['ASX'].get('url')
                self.driver.get(url)
                soup = self.return_soup()
                listing_info = [co.text.strip() for co in soup.find_all('span', attrs={'class': 'gtm-accordion'})]
                df = pd.DataFrame(listing_info)
                df.columns = ['listing_info']
                df['company_name'] = df['listing_info'].str.extract(r'^([a-zA-Z0-9\s,\.&\(\)\-]*)\s\-')
                df['ipo_date'] = df['listing_info'].str.extract(r'\s*-\s*(\d{1,2}\s\w*\s\d{2,4})')
                df['ipo_date'] = pd.to_datetime(df['ipo_date'], errors='coerce').dt.date
                df['exchange'] = 'Australian Stock Exchange'
                df['time_checked'] = self.time_checked_str
                return df
            except Exception as e:
                logger.error(f"ERROR for ASX")
                logger.error(e, exc_info=sys.exc_info())

        def tkipo():
            try:
                url = self.sources['TokyoIPO'].get('url')
                self.driver.get(url)
                soup = self.return_soup()
                table = soup.find('table', attrs={'class': 'iposchedulelist'})
                table_data = []
                row = []
                for r in table.find_all('tr'):
                    for cell in r.find_all('td'):
                        cell_text = cell.text.strip()
                        if '\n\n▶\xa0Stock/Chart' in cell_text:
                            table_data.append(row)
                            row = [cell_text.replace('\n\n▶\xa0Stock/Chart', '')]
                        else:
                            row.append(cell_text)
                table_data.append(row)
                df = pd.DataFrame(table_data)
                df.columns = ['company_name', 'ipo_date', 'ticker', 'shares_outstanding', 'blank_0', 'price_range',
                              'price', 'bookbuilding_period', 'opening_price', 'percent_change', 'underwriters',
                              'business_description', 'blank_1']
                df.replace('', np.nan, inplace=True)
                df.dropna(how='all', inplace=True)
                df.drop(columns=['blank_0', 'business_description', 'blank_1'],  inplace=True, errors='ignore')
                df['company_name'] = df['company_name'].str.strip()
                df['price_range_expected_date'] = df['price_range'].str.extract(r'^(\d{0,2}\/\d{0,2})$')
                df['price_expected_date'] = df['price'].str.extract(r'^(\d{0,2}\/\d{0,2})$')
                df['price'] = pd.to_numeric(df['price'].str.replace(',', ''), errors='coerce')
                # date is provided as mm/dd, adding current year to make the date formatted as mm/dd/yyyy
                df['ipo_date'] = df['ipo_date'] + f"/{datetime.now().year}"
                df['ipo_date'] = pd.to_datetime(df['ipo_date'], errors='coerce').dt.date
                # at the beginning of the year, the calendar will still show IPOs from last year
                # adding the current year to that previous date will be incorrect
                # those incorrect dates will be 6+ months away, shouldn't see legitimate IPO dates that far in advance
                # if the IPO date is more than 6 months away, I subtract 1 year from the IPO date
                df.loc[df['ipo_date'] > (pd.to_datetime('today') + pd.offsets.DateOffset(months=6)), 'ipo_date'] = df['ipo_date'] - pd.offsets.DateOffset(years=1)
                # at the end of the year, the calendar will show IPOs for next year
                # adding the current year to that previous date will be incorrect
                # those incorrect dates will be more than 6 months in the past
                # if the IPO date is less than 6 months past, I add 1 year to the IPO date
                df.loc[df['ipo_date'] < (pd.to_datetime('today') - pd.offsets.DateOffset(months=6)), 'ipo_date'] = df['ipo_date'] + pd.offsets.DateOffset(years=1)
                df['exchange'] = 'Japan Stock Exchange' + ' - ' + df['ticker'].str.extract(r'\((\w*)\)')
                df['ticker'] = df['ticker'].str.replace(r'(\(\w*\))', '', regex=True)
                df['time_checked'] = self.time_checked_str
                return df
            except Exception as e:
                logger.error(f"ERROR for TokyoIPO")
                logger.error(e, exc_info=sys.exc_info())

        def av_api():
            try:
                parameters = self.sources['AlphaVantage'].get('parameters')
                endpoint = self.sources['AlphaVantage'].get('endpoint')
                r = requests.get(endpoint, params=parameters, verify=False)
                if r.ok:
                    cal = [row.replace('\r', '').split(',') for row in r.text.split('\n')]
                    df = pd.DataFrame(cal)
                    df.columns = df.loc[0]
                    df = df.drop(0).reset_index(drop=True)
                    df = df.dropna()
                    if len(df) > 0:
                        df.loc[df['name'].str.contains(r' Warrant'), 'assetType'] = 'Warrants'
                        df.loc[df['name'].str.contains(r' Right'), 'assetType'] = 'Rights'
                        df.loc[df['name'].str.contains(r' Unit'), 'assetType'] = 'Units'
                        df['assetType'].fillna('Shares', inplace=True)
                        for c in ['priceRangeLow', 'priceRangeHigh']:
                            df[c] = pd.to_numeric(df[c], errors='coerce')
                        df['time_checked'] = self.time_checked_str
                        df.sort_values(by=['ipoDate', 'name'], inplace=True)
                        df.rename(columns={
                            'symbol': 'ticker',
                            'name': 'company_name',
                            'ipoDate': 'ipo_date',
                            'priceRangeLow': 'price_range_low',
                            'priceRangeHigh': 'price_range_high'}, inplace=True)
                        return df
            except Exception as e:
                logger.error(f"ERROR for AlphaVantage")
                logger.error(e, exc_info=sys.exc_info())

        def spotlight_api():
            try:
                endpoint = self.sources['SpotlightAPI'].get('endpoint')
                res = requests.get(endpoint)
                if res.ok:
                    rj = json.loads(res.text)
                    df = pd.json_normalize(rj)
                    # this api returns all IPOs so cutting it down to only IPOs since 2020
                    df = df.loc[df['ListingDate'] >= '2020-01-01']
                    # dropping additional documents columns, keeping 'Documents'
                    df.drop(columns=['ExternalDocuments', 'CompanyDocuments'], inplace=True)
                    # Documents is a list (it comes from json) which will throw an error when I try to drop duplicates
                    # TypeError: unhashable type: 'list'
                    # converting Documents to string to avoid that error
                    df['Documents'] = df['Documents'].astype(str)
                    df.rename(columns={
                        'Id': 'num',
                        'DateFrom': 'subscription_date_start',
                        'DateTo': 'subscription_date_end',
                        'ListingDate': 'ipo_date',
                        'CompanyName': 'company_name',
                        'EmissionDescriptionEnglish': 'listing_type'}, inplace=True)
                    df['time_checked'] = self.time_checked_str
                    return df
            except Exception as e:
                logger.error(f"ERROR for SpotlightAPI")
                logger.error(e, exc_info=sys.exc_info())

        def ipohub():
            try:
                url = self.sources['IPOHub'].get('url')
                self.driver.get(url)
                soup = self.return_soup()
                ipo_data = defaultdict(list)
                for ipo in soup.find_all('a', attrs={'class': 'info-card'}):
                    opts = {}
                    for opt in ipo.find_all('div', attrs={'class': 'info-card__option'}):
                        opt_items = opt.find_all('span')
                        opts[opt_items[0].text.strip()] = opt_items[1].text.strip()

                    card_items = {
                        'company_name': ipo.find('div', attrs={'class': 'info-card__title'}).text.strip(),
                        'exchange': ipo.find('div', attrs={'class': 'info-card__company-country'}).text.strip(),
                        'listing_type': ipo.find('span', attrs={'class': 'info-card__tag-item'}).text.strip(),
                        'subscription_period': opts.get('Subscr. period'),
                        'price': opts.get('Price per share'),
                        'market_cap': opts.get('Pre-money valuation'),
                        'deal_size': opts.get('Target to raise'),
                        'status': opts.get('Offer status'),
                        'ipo_date': opts.get('First trading date'),
                    }
                    for k, v in card_items.items():
                        ipo_data[k].append(v)

                df = pd.DataFrame(ipo_data)
                df['ipo_date'] = pd.to_datetime(df['ipo_date'], errors='coerce')
                # the website will provide only a year if they expect the IPO to list some time during the year
                # that is gets interpreted as Jan. 1 of that year when converting to datetime
                # any ipo_date earlier than today (i.e. Jan 1 this year) should not be considered as an actual date
                df.loc[df['ipo_date'] <= datetime.utcnow(), 'ipo_date'] = pd.NaT
                df.loc[df['price'].str.contains('-', na=False), 'price_range'] = df['price']
                df['currency'] = df['price'].str.extract(r"\s(\w{3})")
                df['price'] = pd.to_numeric(df['price'].str.replace(r"(\s\w{3})", '', regex=True), errors='coerce')
                df['time_checked'] = self.time_checked_str
                return df
            except Exception as e:
                logger.error(f"ERROR for IPOHub")
                logger.error(e, exc_info=sys.exc_info())

        special_case_dict = {
            'ASX': asx(),
            'TokyoIPO': tkipo(),
            'AlphaVantage': av_api(),
            'SpotlightAPI': spotlight_api(),
            'IPOHub': ipohub()
        }

        for src, df in special_case_dict.items():
            try:
                if df is not None:
                    s_file = os.path.join(self.source_data_folder, self.sources[src].get('file') + '.csv')
                    if os.path.exists(s_file):
                        df = self.update_existing_data(pd.read_csv(s_file), df, exclude_col='time_checked')
                    df.sort_values(by='time_checked', ascending=False, inplace=True)
                    df.to_csv(s_file, index=False, encoding='utf-8-sig')
                    self.update_table(df, self.sources[src].get('db_table_raw'))
                    self.webscraping_results.append([self.time_checked_str, src, 1])
                else:
                    self.webscraping_results.append([self.time_checked_str, src, 0])
            except Exception as e:
                logger.error(e, exc_info=sys.exc_info())

    @staticmethod
    def update_existing_data(old_df: pd.DataFrame, new_df: pd.DataFrame, exclude_col=None) -> pd.DataFrame:
        """
        If there is already existing data, this function can be called to remove any duplicates.
        :param old_df: DataFrame with existing data
        :param new_df: DataFrame with new data
        :param exclude_col: Column(s) that will be excluded when removing duplicate values in DataFrames.
                            Can be given either as a list of columns or a string with the column name.
        :return: DataFrame
        """
        try:
            df = pd.concat([old_df, new_df.astype(old_df.dtypes)], ignore_index=True, sort=False)
        except KeyError as ke:
            logger.error(ke)
            logger.info(f"Existing df columns: {', '.join(old_df.columns)}")
            logger.info(f"New df columns: {', '.join(new_df.columns)}")
        except ValueError as ve:
            logger.error(ve)
            logger.info(f"Existing df data types: \n{old_df.dtypes.to_string(na_rep='')}")
            logger.info(f"New df data types: \n{new_df.dtypes.to_string(na_rep='')}")
            df = pd.concat([old_df, new_df], ignore_index=True, sort=False)
        if exclude_col and isinstance(exclude_col, str):
            ss = [col for col in df.columns.to_list() if col != exclude_col]
        elif exclude_col and isinstance(exclude_col, list):
            ss = [col for col in df.columns.to_list() if col not in exclude_col]
        else:
            ss = df.columns.to_list()
        # I want to preserve when this item was first added to the website and have most recent updates at the top so
        # sorting by most recent time_checked, dropping duplicates for subset of columns and keeping the last (earliest)
        if 'time_checked' in df.columns:
            df['time_checked'] = pd.to_datetime(df['time_checked'], errors='coerce')
            df.sort_values(by='time_checked', ascending=False, inplace=True)
        df.drop_duplicates(subset=ss, keep='last', inplace=True)
        return df

    def update_table(self, df_new: pd.DataFrame, source_table: str):
        df = df_new.copy()
        try:
            df_s = pd.read_sql_table(source_table, self.conn)
            for col in ['time_checked']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col].fillna(pd.NaT), errors='coerce')
            for col in df.columns:
                if col in df_s.columns:
                    df[col] = df[col].astype(df_s[col].dtype.name)
            merge_cols = [c for c in df.columns if c not in ('time_checked', 'time_added', 'time_removed')]
            df_m = pd.merge(df_s, df, how='outer', on=merge_cols, suffixes=('', '_'), indicator=True)
            df_m['time_added'].fillna(df_m['time_checked'], inplace=True)
            df_m.loc[
                (df_m['_merge'] == 'left_only')
                & (df_m['time_removed'].isna()), 'time_removed'] = self.time_checked_str
            df_m.drop(columns=['_merge', 'time_checked'], inplace=True)
        except ValueError:
            # if the table doesn't exist in the db, it will throw a value error
            df_m = df.rename(columns={'time_checked': 'time_added'})
            df_m['time_removed'] = pd.NaT
        for col in ['time_added', 'time_removed']:
            if col in df_m.columns:
                df_m[col] = pd.to_datetime(df_m[col].fillna(pd.NaT), errors='coerce')
        df_m.sort_values(by='time_added', inplace=True)
        df_m.to_sql(source_table, self.conn, if_exists='replace', index=False, dtype={
            'time_added': sql_types.DateTime,
            'time_removed': sql_types.DateTime
        })
        # logger.info(f"Table {source_table} updated")

    def save_webscraping_results(self):
        """
        Creates a CSV file to log if webscraping was successful for each source
        :return: None
        """
        ws_results_file = os.path.join('Logs', 'Webscraping Results.csv')
        with open(ws_results_file, 'a+', newline='') as f:
            writer = csv.writer(f)
            for r in self.webscraping_results:
                writer.writerow(r)
        try:
            df_wr = pd.DataFrame(self.webscraping_results)
            df_wr.columns = ['time_checked', 'source', 'result']
            df_wr.to_sql('webscraping_results', self.conn, if_exists='append', index=False)
        except Exception as e:
            logger.error(e, exc_info=sys.exc_info())

    def close_down(self):
        self.driver.close()
        self.conn.close()


def main():
    wd = WebDriver()
    wd.random_wait()
    logger.info("Gathering data from sources")
    for k, v in wd.website_sources.items():
        try:
            wd.load_url(v.get('url'), sleep_after=True)
            df = wd.parse_table(**v)
            if df is not None:
                wd.update_table(df, v.get('db_table_raw'))
                s_file = os.path.join(wd.source_data_folder, v.get('file') + '.csv')
                if os.path.exists(s_file):
                    df = wd.update_existing_data(pd.read_csv(s_file), df, exclude_col='time_checked')
                df.sort_values(by='time_checked', ascending=False, inplace=True)
                df.to_csv(s_file, index=False, encoding='utf-8-sig')
                wd.webscraping_results.append([wd.time_checked_str, k, 1])
        except Exception as e:
            logger.error(f"ERROR for {k}")
            logger.error(e, exc_info=sys.exc_info())
            error_screenshot_file = f"{k} Error {wd.time_checked.strftime('%Y-%m-%d %H%M')}.png"
            wd.driver.save_screenshot(os.path.join(log_folder, 'Screenshots', error_screenshot_file))
            wd.webscraping_results.append([wd.time_checked_str, k, 0])
            pass
    wd.special_cases()
    wd.save_webscraping_results()
    wd.close_down()


if __name__ == '__main__':
    main()
