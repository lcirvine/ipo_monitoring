import unittest
from website_scraping import WebDriver
from pathlib import Path
import json

unittest.TestLoader.sortTestMethodsUsing = None

with open(Path.cwd().parent/'sources.json') as f:
    sources = json.load(f)
wd = WebDriver(headless=False, sources=sources)


class WebScrapingTest(unittest.TestCase):

    def test_nyse(self):
        exch = 'NYSE'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_nyse_wd(self):
        exch = 'NYSE Withdrawn'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_nasdaq(self):
        exch = 'Nasdaq'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_nasdaq_priced(self):
        exch = 'Nasdaq Priced'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_nasdaq_wd(self):
        exch = 'Nasdaq Withdrawn'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_jpx(self):
        exch = 'JPX'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_shanghai(self):
        exch = 'Shanghai'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_euronext(self):
        exch = 'Euronext'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_aastocks(self):
        exch = 'AAStocks'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_lse(self):
        exch = 'LSE'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_cninfo(self):
        exch = 'CNInfo'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_frankfurt(self):
        exch = 'Frankfurt'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_krx(self):
        exch = 'KRX'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_twse(self):
        exch = 'TWSE'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_bme(self):
        exch = 'BME'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_sgx(self):
        exch = 'SGX'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_idx(self):
        exch = 'IDX'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_bm(self):
        exch = 'BM'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_bit(self):
        exch = 'BIT'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_iposcoop(self):
        exch = 'IPOScoop'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_nordic(self):
        exch = 'NasdaqNordic'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_eastmoney(self):
        exch = 'East Money'
        exch_data = wd.sources.get(exch)
        wd.load_url(exch_data.get('url'), sleep_after=True)
        df = wd.parse_table(**exch_data)
        if df:
            df.to_csv(Path.cwd()/'Results'/f"{exch}.csv", index=False)
        self.assertIsNotNone(df, f"Unable to create dataframe for {exch}")

    def test_z_close_down(self):
        wd.close_down()
        self.assertIsNotNone(wd.driver.session_id, f"driver should now be closed")


if __name__ == '__main__':
    log_file = 'Test Website Scraping Logs.txt'
    log_folder = Path.cwd().parent/'Logs'
    with open(log_folder/log_file) as f:
        runner = unittest.TextTestRunner(f)
        unittest.main(testRunner=runner)
