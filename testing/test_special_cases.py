import unittest
from pathlib import Path
import json
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import pandas as pd
import numpy as np
import requests
from datetime import datetime
from collections import defaultdict

with open(Path.cwd().parent / 'sources.json') as f:
    sources = json.load(f)
driver = webdriver.Firefox()


class MyTestCase(unittest.TestCase):

    def test_asx(self):
        url = sources['ASX'].get('url')
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        listing_info = [co.text.strip() for co in soup.find_all('h6', attrs={'class': 'gtm-accordion'})]
        df = pd.DataFrame(listing_info)
        df.columns = ['listing_info']
        df['company_name'] = df['listing_info'].str.extract(r'^([a-zA-Z0-9\s,\.&\(\)\-]*)\s\-')
        df['ipo_date'] = df['listing_info'].str.extract(r'\s*-\s*(\d{1,2}\s\w*\s\d{2,4})')
        df['ipo_date'] = pd.to_datetime(df['ipo_date'], errors='coerce')
        df['exchange'] = 'Australian Stock Exchange'
        if df:
            df.to_csv(Path.cwd()/'Results'/'ASX.csv', index=False)
        self.assertIsNotNone(df, 'Unable to create dataframe for ASX')

    def tkipo(self):
        url = sources['TokyoIPO'].get('url')
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
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
        df.drop(columns=['blank_0', 'business_description', 'blank_1'], inplace=True, errors='ignore')
        df['company_name'] = df['company_name'].str.strip()
        df['price_range_expected_date'] = df['price_range'].str.extract(r'^(\d{0,2}\/\d{0,2})$')
        df['price_expected_date'] = df['price'].str.extract(r'^(\d{0,2}\/\d{0,2})$')
        df['price'] = pd.to_numeric(df['price'].str.replace(',', ''), errors='coerce')
        # date is provided as mm/dd, adding current year to make the date formatted as mm/dd/yyyy
        df['ipo_date'] = df['ipo_date'] + f"/{datetime.now().year}"
        df['ipo_date'] = pd.to_datetime(df['ipo_date'], errors='coerce')
        df['exchange'] = 'Japan Stock Exchange' + ' - ' + df['ticker'].str.extract(r'\((\w*)\)')
        df['ticker'] = df['ticker'].str.replace(r'(\(\w*\))', '', regex=True)
        if df:
            df.to_csv(Path.cwd()/'Results'/'TokyoIPO.csv', index=False)
        self.assertIsNotNone(df, 'Unable to create dataframe for TokyoIPO')

    def test_ipohub(self):
        url = sources['IPOHub'].get('url')
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
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
        if df:
            df.to_csv(Path.cwd()/'Results'/'IPOHub.csv', index=False)
        self.assertIsNotNone(df, 'Unable to create dataframe for IPOHub')

    def test_spotlight_api(self):
        endpoint = sources['SpotlightAPI'].get('endpoint')
        res = requests.get(endpoint)
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
        if df:
            df.to_csv(Path.cwd()/'Results'/'Spotlight.csv', index=False)
        self.assertIsNotNone(df, 'Unable to create dataframe for Spotlight')

    def test_av_api(self):
        parameters = sources['AlphaVantage'].get('parameters')
        endpoint = sources['AlphaVantage'].get('endpoint')
        r = requests.get(endpoint, params=parameters, verify=False)
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
            df.sort_values(by=['ipoDate', 'name'], inplace=True)
            df.rename(columns={
                'symbol': 'ticker',
                'name': 'company_name',
                'ipoDate': 'ipo_date',
                'priceRangeLow': 'price_range_low',
                'priceRangeHigh': 'price_range_high'}, inplace=True)
        if df:
            df.to_csv(Path.cwd()/'Results'/'AlphaVantage.csv', index=False)
        self.assertIsNotNone(df, 'Unable to create dataframe for AlphaVantage')

    def test_z_close_down(self):
        driver.quit()
        self.assertIsNotNone(driver.session_id, f"driver should now be closed")


if __name__ == '__main__':
    unittest.main()
