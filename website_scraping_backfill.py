import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
# from selenium.webdriver.support import expected_conditions as EC
import pandas as pd


class BackFill:
    def __init__(self, url: str, headless: bool = True, **kwargs):
        opts = Options()
        if headless:
            opts.headless = True
        self.driver = webdriver.Firefox(options=opts)
        self.sleep_time = 3
        self.url = url
        self.table_data = []
        self.more_avail = True
        self.source = kwargs.get('source', '')
        self.table_elem = kwargs.get('table_elem', 'table')
        self.row_elem = kwargs.get('row_elem', 'tr')
        self.cell_elem = kwargs.get('cell_elem', 'td')
        self.link_elem = kwargs.get('link_elem', 'a')
        self.link_sub = kwargs.get('link_sub', 'href')
        self.link_base = kwargs.get('link_base', '')
        self.next_xpath = kwargs.get('next_xpath')
        self.cols = kwargs.get('cols')
        self.time_stamp = datetime.utcnow().strftime('%Y-%m-%d %H%M')
        self.page_count = 0

    def load_url(self):
        self.driver.get(self.url)
        time.sleep(self.sleep_time)

    def return_soup(self):
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        return soup

    def check_next_button(self):
        if self.next_xpath:
            next_button = self.driver.find_element_by_xpath(self.next_xpath)
            if next_button.is_enabled() and next_button.is_displayed():
                next_button.click()
            else:
                self.more_avail = False

    def parse_table(self, add_link: bool = True):
        self.load_url()
        while self.more_avail:
            soup = self.return_soup()
            table = soup.find(self.table_elem)
            for row in table.find_all(self.row_elem):
                cells = [c.text.strip() for c in row.find_all(self.cell_elem)]
                if add_link:
                    for link in row.find_all(self.link_elem):
                        cells.append(self.link_base + link[self.link_sub])
                if len(cells) > 1:
                    self.table_data.append(cells)
            self.page_count += 1
            if self.page_count % 5 == 0:
                self.save_data()
            time.sleep(self.sleep_time)
            self.check_next_button()
        self.close_driver()

    def close_driver(self):
        self.driver.close()

    def save_data(self):
        backfill_folder = os.path.join(os.getcwd(), 'Backfill')
        if not os.path.exists(backfill_folder):
            os.mkdir(backfill_folder)
        df = pd.DataFrame(self.table_data)
        if self.cols:
            df.columns = self.cols
        backfill_file = os.path.join(backfill_folder, f"{self.source} Backfill {self.time_stamp}.xlsx")
        df.to_excel(backfill_file, sheet_name=f"Backfill {self.source}"[:30], index=False, encoding='utf-8-sig',
                    freeze_panes=(1, 0))


def main():
    source_d = {'source': 'Euronext',
                'link_base': 'https://live.euronext.com',
                'next_xpath': '//*[@title="Go to next page"]',
                'cols': ['Date', 'Company name', 'ISIN code', 'Location', 'Market', 'Link']}
    bf = BackFill(url='https://live.euronext.com/en/ipo-showcase', headless=False, **source_d)
    try:
        bf.parse_table()
        bf.save_data()
    except Exception as e:
        print(e)
    finally:
        bf.close_driver()


if __name__ == '__main__':
    main()
