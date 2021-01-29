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


class WebDriver:
    def __init__(self, headless: bool = True):
        opts = Options()
        if headless:
            opts.headless = True
        self.driver = webdriver.Firefox(options=opts)
        self.sleep_time = 3
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
            df.dropna(how='all', inplace=True)
            # Getting rid of rows that match the column headers in case headers have same element as rows
            c = df.columns[1]
            df = df.loc[df[c] != c]
            df['time_checked'] = self.time_checked_str
            return df

    def close_driver(self):
        self.driver.close()

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
            logger.error(f"ERROR for {k}")
            logger.error(e, exc_info=sys.exc_info())
            logger.info('-' * 100)
            error_screenshot_file = f"{k} Error {wd.time_checked.strftime('%Y-%m-%d %H%M')}.png"
            wd.driver.save_screenshot(os.path.join(log_folder, 'Screenshots', error_screenshot_file))
            wd.webscraping_results.append([wd.time_checked_str, k, 0])
            pass
    wd.close_driver()
    wd.save_webscraping_results()


if __name__ == '__main__':
    main()
