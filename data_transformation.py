import os
import sys
import pandas as pd
import numpy as np
from datetime import date
from logging_ipo_dates import logger, log_folder

pd.options.mode.chained_assignment = None


class DataTransformation:
    def __init__(self):
        self.source_folder = os.path.join(os.getcwd(), 'Data from Sources')
        self.final_cols = ['Company Name', 'Symbol', 'Market', 'IPO Date', 'Price', 'Price Range', 'Status', 'time_checked', 'Notes']
        self.df_all = pd.DataFrame(columns=self.final_cols)
        self.result_folder = os.path.join(os.getcwd(), 'Results')
        if not os.path.exists(self.result_folder):
            os.mkdir(self.result_folder)
        self.result_file = os.path.join(self.result_folder, 'All IPOs.xlsx')
        self.writer = pd.ExcelWriter(self.result_file, date_format='%Y-%m-%d')
        self.src_dfs = self.all_source_files()

    def all_source_files(self):
        src_dfs = {os.path.splitext(f)[0]: pd.read_csv(os.path.join(self.source_folder, f)) for f in
                   os.listdir(self.source_folder) if os.path.splitext(f)[1] == '.csv'}
        return src_dfs

    def append_to_all(self, df_exch: pd.DataFrame):
        for c in [col for col in self.final_cols if col not in df_exch.columns]:
            df_exch[c] = np.nan
        df_exch = df_exch[self.final_cols]
        self.df_all = pd.concat([self.df_all, df_exch], ignore_index=True, sort=False)

    @staticmethod
    def format_date_cols(df: pd.DataFrame, date_cols: list, dayfirst=False):
        for c in date_cols:
            df[c] = pd.to_datetime(df[c], errors='coerce', dayfirst=dayfirst)
        return df

    def nyse(self):
        file_name = 'NYSE'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df_up = self.src_dfs.get([file_name]).copy()
        df_up = self.format_date_cols(df_up, ['Expected Date', 'time_checked'])
        df_up.rename(columns={'Curr. File Price/Range($)': 'Price Range'}, inplace=True)

        file_name = 'NYSE Withdrawn'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df_wd = self.src_dfs.get([file_name]).copy()
        df_wd = self.format_date_cols(df_wd, ['Date W/P', 'time_checked'])
        df_wd['Notes'] = 'Withdrawn on ' + df_wd['Date W/P'].astype(str)

        df = pd.concat([df_up, df_wd], ignore_index=True, sort=False)
        df.rename(columns={'Issuer': 'Company Name', 'Ticker': 'Symbol', 'Expected Date': 'IPO Date',
                           'Exchange': 'Market'}, inplace=True)
        self.append_to_all(df)

    def nasdaq(self):
        file_name = 'Nasdaq'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df_up = self.src_dfs.get([file_name]).copy()
        df_up = self.format_date_cols(df_up, ['Expected IPO Date', 'time_checked'])
        df_up.rename(columns={'Exchange/ Market': 'Market', 'Expected IPO Date': 'IPO Date'}, inplace=True)
        df_up.loc[df_up['Price'].str.contains('-', na=False), 'Price Range'] = df_up['Price']
        df_up.loc[df_up['Price'].str.contains('-', na=False), 'Price'] = np.nan

        file_name = 'Nasdaq Priced'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df_p = self.src_dfs.get([file_name]).copy()
        df_p = self.format_date_cols(df_p, ['Date', 'time_checked'])
        df_p.rename(columns={'Exchange/ Market': 'Market', 'Date': 'IPO Date', 'Actions': 'Status'}, inplace=True)

        file_name = 'Nasdaq Withdrawn'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df_wd = self.src_dfs.get([file_name]).copy()
        df_wd = self.format_date_cols(df_wd, ['Date Filed', 'Date Withdrawn', 'time_checked'])
        df_wd['Notes'] = 'Withdrawn on ' + df_wd['Date Withdrawn'].astype(str)
        df_wd['Status'] = 'Withdrawn'

        df = pd.concat([df_up, df_p, df_wd], ignore_index=True, sort=False)
        self.append_to_all(df)

    def iposcoop(self):
        file_name = 'IPOScoop'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df['IPO Date'] = df['Expected to Trade'].str.extract(r'(\d{1,2}/\d{1,2}/\d{4})')
        df = self.format_date_cols(df, ['IPO Date', 'time_checked'])
        df['Status'] = df['Expected to Trade'].str.extract(r'(Priced|Postponed)')
        df.loc[df['Price Low'] != df['Price High'], 'Price Range'] = df['Price Low'].astype(str) + ' - ' + df['Price High'].astype(str)
        df.loc[df['Price Low'] == df['Price High'], 'Price'] = df['Price High']
        df['Notes'] = df['Expected to Trade'].str.extract(r'(Week of)')
        df['Market'] = 'IPOScoop'
        df.rename(columns={'Company': 'Company Name', 'Symbol proposed': 'Symbol'}, inplace=True)
        self.append_to_all(df)

    def jpx(self):
        file_name = 'JPX'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['Date of Listing', 'Date of Listing Approval', 'time_checked'])
        df['Market'] = 'Japan Stock Exchange - ' + df['Market Division']
        df.rename(columns={'Date of Listing': 'IPO Date', 'Issue Name': 'Company Name', 'Code': 'Symbol'}, inplace=True)
        self.append_to_all(df)

    def shanghai(self):
        file_name = 'Shanghai'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['Subscription Date', 'Announcement Day of Winning Results', 'Listing date',
                                        'time_checked'])
        df['Company Name'] = df['New Share Name'].str.extract(r'^(\w*)\s')
        df['Symbol'] = df['Company Name'].str.extract(r'\w(\d*)\b')
        df['Company Name'] = df['Company Name'].str.replace(r'\w(\d*)\b', '')
        df['Market'] = 'Shanghai Stock Exchange'
        df.rename(columns={'Listing date': 'IPO Date', 'Issue price': 'Price'}, inplace=True)
        self.append_to_all(df)

    def euronext(self):
        file_name = 'Euronext'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['Date'], dayfirst=True)
        df = self.format_date_cols(df, ['time_checked'])
        df['Market'] = df['Market'] + ' ' + df['Location']
        df.rename(columns={'Date': 'IPO Date', 'Company name': 'Company Name', 'ISIN code': 'Symbol'}, inplace=True)
        self.append_to_all(df)

    def aastocks(self):
        file_name = 'AAStocks'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['Listing Date', 'time_checked'])
        df['Market'] = 'Hong Kong Stock Exchange'
        df['Symbol'] = df['Code▼'].str.extract(r'(\d*)\.HK')
        df.loc[df['Offer Price'].str.contains('-', na=False), 'Price Range'] = df['Offer Price']
        df.loc[~df['Offer Price'].str.contains('-', na=False), 'Price'] = df['Offer Price']
        df.rename(columns={'Name': 'Company Name', 'Listing Date': 'IPO Date'}, inplace=True)
        self.append_to_all(df)

    def lse(self):
        file_name = 'LSE'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['Expected first date of trading', 'time_checked'])
        df['Market'] = 'London Stock Exchange ' + df['Market'].fillna('')
        df.rename(columns={'Name': 'Company Name', 'Expected first date of trading': 'IPO Date', 'Price range': 'Price Range'}, inplace=True)
        self.append_to_all(df)

    def cninfo(self):
        file_name = 'CNInfo'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['Listing date', 'Release date', 'time_checked'])
        df['Market'] = 'Shenzhen Stock Exchange'
        df.rename(columns={'Code': 'Symbol', 'Abbreviation': 'Company Name', 'Issue price': 'Price',
                           'Listing date': 'IPO date'}, inplace=True)
        self.append_to_all(df)

    def tsx(self):
        file_name = 'TSX'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['Date', 'time_checked'])
        df['Symbol'] = df['Company'].str.extract(r'\(([a-zA-Z\.,\s]*)\)')
        df['Company Name'] = df['Company'].str.extract(r'^([a-zA-Z\.\s\d&,]*)[\\xa0|\(]')
        df['Company Name'] = df['Company Name'].str.strip()
        df.rename(columns={'Date': 'IPO Date'}, inplace=True)
        self.append_to_all(df)

    def nse(self):
        file_name = 'NSE'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()

    def bse(self):
        file_name = 'BSE'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['Start Date', 'End Date', 'time_checked'])
        df.rename(columns={'End Date': 'IPO Date', 'Security Name': 'Company Name', 'Offer Price': 'Price'}, inplace=True)
        df = df.loc[~df['Type Of Issue'].isin(['Debt Issue', 'BuyBack', 'RI', 'Buyback - Tender Offer', 'Takeover'])]
        self.append_to_all(df)

    def frankfurt(self):
        file_name = 'Frankfurt'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['Date', 'time_checked'])
        rows_to_shift = df.loc[(df['First Price and Market Cap'].isna()) &
                               (~df['Sub Price and Deal Size'].isna())].index.to_list()
        df.iloc[rows_to_shift, 4:-1] = df.iloc[rows_to_shift, 4:-1].shift(1, axis=1)
        df['Company Name'] = df['Summary'].str.extract(r'\)([a-zA-Z\s\d\-&\.,]*)Sector')
        df['Offer Type'] = df['Market'].str.extract(r'\(([a-zA-Z\s\/]*)\)')
        df = df.loc[df['Offer Type'] != 'Transfer']
        df['Notes'] = 'Offer Type: ' + df['Offer Type'].fillna('')
        df['Market'] = 'Frankfurt Stock Exchange - ' + df['Market'].str.extract(r'^([a-zA-Z\s]*)\s\(')
        df['First Traded Price'] = df['First Price and Market Cap'].str.extract(r'Quotation: €\s(\d{1,3}\.\d{1,3})')
        df['Market Cap'] = df['First Price and Market Cap'].str.extract(r'/\s€\s([\d,\.]*)')
        df['Price'] = df['Sub Price and Deal Size'].str.extract(r'Volume: €\s(\d{1,3}\.\d{1,3})')
        df['Deal Size'] = df['Sub Price and Deal Size'].str.extract(r'/\s€\s([\d,\.]*)')
        df['Sector'] = df['Sector'].str.extract(r'Sector:\n\t\t\t([a-zA-|&,\.\s]*)')
        df.rename(columns={'Date': 'IPO Date'}, inplace=True)
        self.append_to_all(df)

    def krx(self):
        file_name = 'KRX'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['Initial listing date', 'time_checked'])
        df['Market'] = 'Korea Exchange'
        df.rename(columns={'Code': 'Symbol', 'Name': 'Company Name', 'Initial listing date': 'IPO Date',
                           'Public Offering Price(KRW)': 'Price'}, inplace=True)
        self.append_to_all(df)

    def asx(self):
        file_name = 'ASX'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()

    def twse(self):
        file_name = 'TWSE'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['Application Date', 'Date of the Listing Review Committee',
                                        'Date the application approved by the TWSE Board',
                                        'Date of the Agreement for Listing submitted to the FSC for recordation',
                                        'Listing Date', 'time_checked'])
        df['Market'] = 'Taiwan Stock Exchange'
        df.rename(columns={'Code': 'Symbol', 'Company': 'Company Name', 'Listing date': 'IPO Date',
                           'Underwriting price': 'Price'}, inplace=True)
        self.append_to_all(df)

    def bme(self):
        file_name = 'BME'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['New Listing Date', 'time_checked'])
        df['Price'] = df['Turnover'].str.replace(',', '').astype(float) / df['Shares'].str.replace(',', '').astype(
            float)
        df['Market'] = 'Bolsa de Madrid'
        df = df.loc[df['Type'] != 'Integration']
        df.rename(columns={'ISIN': 'Symbol', 'Security': 'Company Name', 'New Listing Date': 'IPO Date'}, inplace=True)
        self.append_to_all(df)

    def sgx(self):
        file_name = 'SGX'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['Listing Date', 'time_checked'])
        df['Market'] = 'Singapore Exchange - ' + df['Listing Board'].fillna('')
        df.rename(columns={'Listing Date': 'IPO Date', 'Offer Price': 'Price'}, inplace=True)
        self.append_to_all(df)

    def idx(self):
        file_name = 'IDX'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['Listing Date', 'time_checked'])
        df['Market'] = 'Indonesia Stock Exchange - ' + df['Listing Board'].fillna('')
        df.rename(columns={'Name': 'Company Name', 'Code or Company Name': 'Symbol', 'Listing Date': 'IPO Date'}, inplace=True)
        self.append_to_all(df)

    def bm(self):
        file_name = 'BM'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['DATE OF LISTING (* Tentative)', 'time_checked'])
        df['Market'] = 'Bursa Malaysia - ' + df['LISTING SOUGHT'].fillna('')
        df['Price'] = df['ISSUE PRICE'].str.extract(r'(\d*\.\d*)')
        df.rename(columns={'NAME OF COMPANY': 'Company Name', 'DATE OF LISTING (* Tentative)': 'IPO Date'}, inplace=True)
        self.append_to_all(df)

    def ipohub(self):
        file_name = 'IPOHub'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        # self.append_to_all(df)

    def formatting_all(self):
        self.df_all.loc[self.df_all['time_checked'] == self.df_all['time_checked'].max(), 'Notes'] = \
            'NEW ' + self.df_all['Notes'].fillna('')
        self.df_all = self.format_date_cols(self.df_all, ['IPO Date', 'time_checked'])
        self.df_all['IPO Date'] = self.df_all['IPO Date'].dt.date
        self.df_all['IPO Date'] = self.df_all['IPO Date'].dt.strftime('%Y-%m-%d')
        self.df_all.loc[self.df_all['IPO Date'] > date.today(), 'Status'] = 'Upcoming ' + self.df_all['Status']
        self.df_all.loc[self.df_all['IPO Date'] == date.today(), 'Status'] = 'Listing Today ' + self.df_all['Status']
        self.df_all.sort_values(by='time_checked', ascending=False, inplace=True)
        self.df_all.drop_duplicates(subset='Company Name', keep='first', inplace=True)
        self.df_all.sort_values(by=['IPO Date', 'time_checked'], ascending=False, inplace=True)
        self.df_all.reset_index(drop=True, inplace=True)

    def save_all(self):
        self.df_all.to_excel(self.writer, sheet_name='All IPOs', index=False, encoding='utf-8-sig', freeze_panes=(1, 0))
        for k, v in self.src_dfs.items():
            v.to_excel(self.writer, sheet_name=k, index=False, encoding='utf-8-sig', freeze_panes=(1, 0))
        self.writer.sheets['All IPOs'].activate()
        self.writer.save()


def main():
    try:
        dt = DataTransformation()
        dt.nyse()
        dt.nasdaq()
        dt.iposcoop()
        dt.jpx()
        dt.shanghai()
        dt.euronext()
        dt.aastocks()
        dt.lse()
        dt.cninfo()
        dt.tsx()
        # dt.nse()
        dt.bse()
        dt.frankfurt()
        dt.krx()
        # dt.asx()
        dt.twse()
        dt.bme()
        dt.sgx()
        dt.idx()
        dt.bm()
        # dt.ipohub()
        dt.formatting_all()
        dt.save_all()
    except Exception as e:
        print(e)
        logger.error(e, exc_info=sys.exc_info())
        logger.info('-' * 100)


if __name__ == '__main__':
    main()


def hk():
    print('HK')





def korea():
    print('Korea')


def lse():
    print('LSE')


def madrid():
    print('Madrid')


def shanghai():
    print('Shanghai')


def shenzhen():
    print('Shenzhen')


def swiss():
    print('Swiss')


def tokyo():
    print('Tokyo')


def frankfurt():
    print('Frankfurt')



