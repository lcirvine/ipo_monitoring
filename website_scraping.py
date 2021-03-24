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
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import configparser


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
                self.sources_dict = json.load(f)
        self.webscraping_results = []
        self.config = configparser.ConfigParser()
        self.config.read('api_key.ini')

    @staticmethod
    def random_wait():
        wait_time = randint(0, 300)
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

    def check_tables_in_sources(self, result_file: str = 'tables in sources') -> None:
        """
        This function is used for testing. It will create a JSON file with data about all the sites in the sources.ini
        file, including the number of tables, their attributes, the number of rows and the column names.
        :param result_file: Name of the JSON file that will be created.
        :return: None. The data is saved in a file.
        """
        dict_all_sources = {}
        for k, v in self.sources_dict.items():
            try:
                dict_source = {'source': k}
                url = v.get('url')
                if url != '':
                    dict_source['url'] = url
                    self.load_url(url)
                    soup = self.return_soup()
                    tables = soup.find_all('table')
                    dict_source['num_tables'] = len(tables)
                    dict_source['tables'] = {}
                    tbl_ct = 0
                    for table in tables:
                        dict_source['tables'][tbl_ct] = {'attrs': table.attrs,
                                                         'rows': len(table.find_all('tr')),
                                                         'columns': [col.text.strip() for col in table.find_all('th')],
                                                         'text': table.text}
                        tbl_ct += 1
                dict_all_sources[k] = dict_source
            except Exception as e:
                print(f"{k} ERROR:\n{e}")
                pass
        self.driver.close()
        with open(result_file + '.json', 'w') as f:
            json.dump(dict_all_sources, f)

    def parse_table(self, get_links: bool = False, **kwargs) -> pd.DataFrame:
        """
        Parses the element identified by the keyword arguments and returns a pandas dataframe
        :param get_links: bool, if true the function will add links to data returned
        :return: pandas dataframe
        """
        url = kwargs.get('url')
        table_elem = kwargs.get('table_elem')
        table_num = kwargs.get('table_num')
        table_attrs = kwargs.get('table_attrs')
        row_elem = kwargs.get('row_elem')
        cell_elem = kwargs.get('cell_elem')
        header_elem = kwargs.get('header_elem')
        link_elem = kwargs.get('link_elem')
        link_key = kwargs.get('link_key')
        cols = kwargs.get('columns')

        soup = self.return_soup()
        if table_attrs is None:
            table = soup.find_all(table_elem)[table_num]
        else:
            table = soup.find(table_elem, attrs=table_attrs)
        assert table is not None, f'Unable to find {table_elem} with these attributes {table_attrs} on {url}'
        table_data = []
        for row in table.find_all(row_elem):
            cells = [c.text.strip() for c in row.find_all(cell_elem)]
            # table_data.append(row.stripped_strings)
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
            # Getting rid of rows that match the column headers in case headers have same element as rows
            c = df.columns[1]
            df = df.loc[df[c] != c]
            df['time_checked'] = self.time_checked_str
            return df

    def asx(self):
        try:
            self.driver.get('https://www2.asx.com.au/listings/upcoming-floats-and-listings')
            soup = self.return_soup()
            listing_info = [co.text.strip() for co in soup.find_all('span', attrs={'class': 'gtm-accordion'})]
            df = pd.DataFrame(listing_info)
            df.columns = ['listing_info']
            df['Company Name'] = df['listing_info'].str.extract(r'^([a-zA-Z0-9\s,\.&]*)\s\-')
            df['IPO Date'] = df['listing_info'].str.extract(r'\s*-\s*(\d{1,2}\s\w*\s\d{2,4})')
            df['IPO Date'] = pd.to_datetime(df['IPO Date'], errors='coerce').dt.date
            df['Market'] = 'Australia Stock Exchange'
            df['time_checked'] = self.time_checked_str
            if df is not None:
                s_file = os.path.join(self.source_data_folder, 'ASX.csv')
                if os.path.exists(s_file):
                    df = self.update_existing_data(pd.read_csv(s_file), df, exclude_col='time_checked')
                df.sort_values(by='time_checked', ascending=False, inplace=True)
                df.to_csv(s_file, index=False, encoding='utf-8-sig')
                self.webscraping_results.append([self.time_checked_str, 'ASX', 1])
        except Exception as e:
            logger.error(f"ERROR for ASX")
            logger.error(e, exc_info=sys.exc_info())
            logger.info('-' * 100)
            error_screenshot_file = f"ASX Error {self.time_checked.strftime('%Y-%m-%d %H%M')}.png"
            self.driver.save_screenshot(os.path.join(log_folder, 'Screenshots', error_screenshot_file))
            self.webscraping_results.append([self.time_checked_str, 'ASX', 0])

    def tkipo(self):
        try:
            self.driver.get('http://www.tokyoipo.com/top/iposche/index.php?j_e=E')
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
            df.columns = ['Company Name', 'IPO Date', 'Symbol', 'Listed Shares', 'Blank_0', 'Price Range', 'Price',
                          'Book Building Period', 'Opening Price', 'Change', 'Lead Underwriter', 'Business Description',
                          'Blank_1']
            df.replace('', np.nan, inplace=True)
            df.dropna(how='all', inplace=True)
            df.drop(columns=['Blank_0', 'Business Description', 'Blank_1'],  inplace=True, errors='ignore')
            df['Company Name'] = df['Company Name'].str.strip()
            df['Price Range Expected Date'] = df['Price Range'].str.extract(r'^(\d{0,2}\/\d{0,2})$')
            df['Price Expected Date'] = df['Price'].str.extract(r'^(\d{0,2}\/\d{0,2})$')
            df['Price'] = pd.to_numeric(df['Price'].str.replace(',', ''), errors='coerce')
            # date is provided as mm/dd, adding current year to make the date formatted as mm/dd/yyyy
            df['IPO Date'] = df['IPO Date'] + f"/{datetime.now().year}"
            df['IPO Date'] = pd.to_datetime(df['IPO Date'], errors='coerce').dt.date
            # at the beginning of the year, the calendar will still show IPOs from last year
            # adding the current year to that previous date will be incorrect
            # those incorrect dates will be 6+ months away, we shouldn't see legitimate IPO dates that far in advance
            # if the IPO date is more than 6 months away, I subtract 1 year from the IPO date
            df.loc[df['IPO Date'] > (pd.to_datetime('today') + pd.offsets.DateOffset(months=6)), 'IPO Date'] = df['IPO Date'] - pd.offsets.DateOffset(years=1)
            df['Market'] = 'Japan Stock Exchange' + ' - ' + df['Symbol'].str.extract(r'\((\w*)\)')
            df['Symbol'] = df['Symbol'].str.replace(r'(\(\w*\))', '', regex=True)
            df['time_checked'] = self.time_checked_str
            if df is not None:
                s_file = os.path.join(self.source_data_folder, 'TokyoIPO.csv')
                if os.path.exists(s_file):
                    df = self.update_existing_data(pd.read_csv(s_file), df, exclude_col='time_checked')
                df.sort_values(by='time_checked', ascending=False, inplace=True)
                df.to_csv(s_file, index=False, encoding='utf-8-sig')
                self.webscraping_results.append([self.time_checked_str, 'TokyoIPO', 1])
        except Exception as e:
            logger.error(f"ERROR for TokyoIPO")
            logger.error(e, exc_info=sys.exc_info())
            logger.info('-' * 100)
            error_screenshot_file = f"TokyoIPO Error {self.time_checked.strftime('%Y-%m-%d %H%M')}.png"
            self.driver.save_screenshot(os.path.join(log_folder, 'Screenshots', error_screenshot_file))
            self.webscraping_results.append([self.time_checked_str, 'TokyoIPO', 0])

    def close_driver(self):
        self.driver.close()

    def av_api(self):
        try:
            requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
            parameters = {'function': self.config.get('AV', 'funct'),
                          'apikey': self.config.get('AV', 'funct')}
            r = requests.get(self.config.get('AV', 'base_url'), params=parameters, verify=False)
            cal = [[cell.replace('\r', '') for cell in row.split(',')] for row in r.text.split('\n')]
            df = pd.DataFrame(cal)
            df.columns = df.loc[0]
            df = df.drop(0).reset_index(drop=True)
            df = df.dropna()
            df.loc[df['name'].str.contains(r' Warrant'), 'assetType'] = 'Warrants'
            df.loc[df['name'].str.contains(r' Right'), 'assetType'] = 'Rights'
            df.loc[df['name'].str.contains(r' Unit'), 'assetType'] = 'Units'
            df.loc[df['assetType'].isna(), 'assetType'] = 'Shares'
            for c in ['priceRangeLow', 'priceRangeHigh']:
                df[c] = pd.to_numeric(df[c], errors='coerce')
            df['time_checked'] = self.time_checked_str
            df.sort_values(by=['ipoDate', 'name'], inplace=True)
            s_file = os.path.join(self.source_data_folder, self.config.get('AV', 'file_name') + '.csv')
            if os.path.exists(s_file):
                df = self.update_existing_data(pd.read_csv(s_file), df, exclude_col='time_checked')
            df.sort_values(by='time_checked', ascending=False, inplace=True)
            df.to_csv(s_file, index=False, encoding='utf-8-sig')
            self.webscraping_results.append([self.time_checked_str, self.config.get('AV', 'file_name'), 1])
        except Exception as e:
            logger.error(f"ERROR for AV")
            logger.error(e, exc_info=sys.exc_info())
            logger.info('-' * 100)
            self.webscraping_results.append([self.time_checked_str, 'AV', 0])

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
        df = pd.concat([old_df, new_df], ignore_index=True, sort=False)
        if exclude_col and isinstance(exclude_col, str):
            ss = [col for col in df.columns.to_list() if col != exclude_col]
        elif exclude_col and isinstance(exclude_col, list):
            ss = [col for col in df.columns.to_list() if col not in exclude_col]
        else:
            ss = df.columns.to_list()
        df.drop_duplicates(subset=ss, inplace=True)
        return df

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


def main():
    wd = WebDriver()
    wd.random_wait()
    for k, v in wd.sources_dict.items():
        try:
            wd.load_url(v.get('url'), sleep_after=True)
            df = wd.parse_table(**v)
            if df is not None:
                s_file = os.path.join(wd.source_data_folder, v.get('file') + '.csv')
                if os.path.exists(s_file):
                    df = wd.update_existing_data(pd.read_csv(s_file), df, exclude_col='time_checked')
                df.sort_values(by='time_checked', ascending=False, inplace=True)
                df.to_csv(s_file, index=False, encoding='utf-8-sig')
                wd.webscraping_results.append([wd.time_checked_str, k, 1])
        except Exception as e:
            # ToDo: If there's an exception, the key is the NEXT key rather than the one that threw the exception. Why?
            logger.error(f"ERROR for {k}")
            logger.error(e, exc_info=sys.exc_info())
            logger.info('-' * 100)
            error_screenshot_file = f"{k} Error {wd.time_checked.strftime('%Y-%m-%d %H%M')}.png"
            wd.driver.save_screenshot(os.path.join(log_folder, 'Screenshots', error_screenshot_file))
            wd.webscraping_results.append([wd.time_checked_str, k, 0])
            pass
    wd.asx()
    wd.tkipo()
    wd.close_driver()
    wd.av_api()
    wd.save_webscraping_results()


if __name__ == '__main__':
    main()
