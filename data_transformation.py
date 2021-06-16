import os
import sys
import pandas as pd
import numpy as np
from datetime import date, datetime
from logging_ipo_dates import logger, error_email

pd.options.mode.chained_assignment = None


class DataTransformation:
    def __init__(self):
        self.source_folder = os.path.join(os.getcwd(), 'Data from Sources')
        self.final_cols = ['company_name', 'ticker', 'exchange', 'ipo_date', 'price', 'price_range', 'shares_offered',
                           'status', 'notes', 'time_checked']
        self.df_all = pd.DataFrame(columns=self.final_cols)
        self.result_folder = os.path.join(os.getcwd(), 'Results')
        if not os.path.exists(self.result_folder):
            os.mkdir(self.result_folder)
        self.result_file = os.path.join(self.result_folder, 'All IPOs.xlsx')
        self.src_dfs = self.all_source_files()

    def all_source_files(self):
        src_dfs = {os.path.splitext(f)[0]: pd.read_csv(os.path.join(self.source_folder, f)) for f in
                   os.listdir(self.source_folder) if os.path.splitext(f)[1] == '.csv'}
        return src_dfs

    def add_missing_cols(self, df_exch: pd.DataFrame) -> pd.DataFrame:
        for c in [col for col in self.final_cols if col not in df_exch.columns]:
            df_exch[c] = np.nan
        return df_exch

    def append_to_all(self, df_exch: pd.DataFrame):
        df_exch = self.add_missing_cols(df_exch)
        df_exch = df_exch[self.final_cols]
        self.df_all = pd.concat([self.df_all, df_exch], ignore_index=True, sort=False)

    @staticmethod
    def format_date_cols(df: pd.DataFrame, date_cols: list, dayfirst=False):
        for c in date_cols:
            df[c] = pd.to_datetime(df[c].fillna(pd.NaT), errors='coerce', dayfirst=dayfirst)
        return df

    def us(self):

        def nyse():
            file_name = 'NYSE'
            assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
            df_up = self.src_dfs.get(file_name).copy()
            df_up = self.format_date_cols(df_up, ['ipo_date', 'time_checked'])
            # NYSE provides the expected pricing date, the expected listing date is one day after
            df_up['ipo_date'] = df_up['ipo_date'] + pd.offsets.DateOffset(days=1)
            df_up['shares_offered'] = df_up['shares_offered'].str.replace(',', '').astype(int, errors='ignore')
            df_up['deal_size'] = df_up['deal_size'].str.replace(',', '').astype(int, errors='ignore')

            file_name = 'NYSE Withdrawn'
            assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
            df_wd = self.src_dfs.get(file_name).copy()
            df_wd = self.format_date_cols(df_wd, ['postponement_date', 'time_checked'])
            df_wd['notes'] = 'Withdrawn on ' + df_wd['postponement_date'].astype(str)
            df_wd['exchange'] = 'NYSE'
            df_wd['shares_offered'] = df_wd['shares_offered'].str.replace(',', '').astype(int, errors='ignore')

            df = pd.concat([df_up, df_wd], ignore_index=True, sort=False)
            return df

        def nasdaq():
            file_name = 'Nasdaq'
            assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
            df_up = self.src_dfs.get(file_name).copy()
            df_up = self.format_date_cols(df_up, ['ipo_date', 'time_checked'])
            df_up.loc[df_up['price'].str.contains('-', na=False), 'price_range'] = df_up['price']
            df_up.loc[df_up['price'].str.contains('-', na=False), 'price'] = np.nan
            df_up['shares_offered'] = df_up['shares_offered'].str.replace(',', '').astype(int, errors='ignore')

            file_name = 'Nasdaq Priced'
            assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
            df_p = self.src_dfs.get(file_name).copy()
            df_p = self.format_date_cols(df_p, ['ipo_date', 'time_checked'])
            df_p['shares_offered'] = df_p['shares_offered'].str.replace(',', '').astype(int, errors='ignore')

            file_name = 'Nasdaq Withdrawn'
            assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
            df_wd = self.src_dfs.get(file_name).copy()
            df_wd = self.format_date_cols(df_wd, ['announcement_date', 'cancellation_date', 'time_checked'])
            df_wd['notes'] = 'Withdrawn on ' + df_wd['cancellation_date'].astype(str)
            df_wd['status'] = 'Withdrawn'
            df_wd['exchange'] = 'Nasdaq'
            df_wd['shares_offered'] = df_wd['shares_offered'].str.replace(',', '').astype(int, errors='ignore')

            df = pd.concat([df_up, df_p, df_wd], ignore_index=True, sort=False)
            return df

        def iposcoop():
            file_name = 'IPOScoop'
            assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
            df = self.src_dfs.get(file_name).copy()
            df['status'] = df['ipo_date'].str.extract(r'(Priced|Postponed)')
            # dropping 'week of' dates because they're just not accurate enough
            # df['notes'] = df['ipo_date'].str.extract(r'(Week of)')
            df = df.loc[~df['ipo_date'].str.contains('Week of')]
            df['ipo_date'] = df['ipo_date'].str.extract(r'(\d{1,2}/\d{1,2}/\d{4})')
            df = self.format_date_cols(df, ['ipo_date', 'time_checked'])
            df.loc[df['price_range_low'] != df['price_range_high'], 'price_range'] = df['price_range_low'].astype(str) + ' - ' + df['price_range_high'].astype(str)
            df.loc[df['price_range_low'] == df['price_range_high'], 'price'] = df['price_range_high']
            df['exchange'] = 'IPOScoop'
            df['shares_offered'] = df['shares_offered_mm'].astype(int, errors='ignore') * 1000000
            return df

        def av():
            file_name = 'AlphaVantage'
            assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
            df = self.src_dfs.get(file_name).copy()
            df = self.format_date_cols(df, ['ipo_date', 'time_checked'])
            df.loc[(df['price_range_low'] != df['price_range_high']) &
                   (df['price_range_low'] != 0), 'price_range'] = df['price_range_low'].astype(str) + ' - ' + df['price_range_high'].astype(str)
            df.loc[(df['price_range_low'] == df['price_range_high']) & (df['price_range_low'] != 0), 'price'] = df['price_range_high']
            # When listing an ETF, warrants or rights both price high and price low will be 0. Those should be dropped.
            # However, direct listings will also have price high and low at 0.
            # TODO: keep only direct listings but drop all other rows where price high and low both equal 0
            # The problem is that when units split into shares and warrants, those will also have 0 as price range
            # and the asset type will still be shares. Those should be dropped as well.
            df.drop(df.loc[(df['price_range_low'] == 0) & (df['price_range_high'] == 0)].index, inplace=True)
            return df

        df_ny = nyse()
        df_nd = nasdaq()
        df_is = iposcoop()
        df_av = av()
        df = pd.concat([df_ny, df_nd], ignore_index=True, sort=False)

        # Add iposcoop and av only when there is not already a row from one of the exchanges
        # company names are often different so joining on symbol after removing special characters
        special_chars = r"(\.|'|\*)"
        df['formatted symbol'] = df['ticker'].str.replace(special_chars, "", regex=True)
        
        def add_only_new(data_frame: pd.DataFrame):
            data_frame['formatted symbol'] = data_frame['ticker'].str.replace(special_chars, "", regex=True)
            data_frame = pd.merge(data_frame, df, how='outer', on='formatted symbol', suffixes=('', '_drop'), indicator=True)
            data_frame = data_frame.loc[data_frame['_merge'] == 'left_only']
            drop_cols = [c for c in data_frame.columns if '_drop' in c]
            drop_cols.append('_merge')
            data_frame.drop(columns=drop_cols, inplace=True)
            return data_frame

        df_is = add_only_new(df_is)
        df = pd.concat([df, df_is], ignore_index=True, sort=False)
        df_av = add_only_new(df_av)
        df = pd.concat([df, df_av], ignore_index=True, sort=False)

        df.sort_values(by=['time_checked'], ascending=False, inplace=True)
        df.drop_duplicates(subset=['company_name', 'ticker'], inplace=True)
        df.loc[df['ipo_date'] >= pd.to_datetime('today'), 'notes'] = 'Price expected ' + (
                df['ipo_date'] - pd.offsets.DateOffset(days=1)).astype(str)
        self.append_to_all(df)

    def jpx(self):
        file_name = 'JPX'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df_jp = self.src_dfs.get(file_name).copy()
        df_jp = self.format_date_cols(df_jp, ['ipo_date', 'date_of_listing_approval', 'time_checked'])
        df_jp['exchange'] = 'Japan Stock Exchange - ' + df_jp['market_segment']
        df_jp.loc[df_jp['company_name'].str.contains(r'\*\*', regex=True), 'notes'] = 'Technical Listing'
        df_jp['company_name'] = df_jp['company_name'].str.replace(r',', ', ')
        df_jp['company_name'] = df_jp['company_name'].str.replace(r'\*\*', '', regex=True)
        df_jp['company_name'] = df_jp['company_name'].str.strip()
        
        file_name = 'TokyoIPO'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df_tk = self.src_dfs.get(file_name).copy()
        df_tk = self.format_date_cols(df_tk, ['ipo_date', 'time_checked'])
        df_tk.loc[df_tk['price_range_expected_date'].notna(), 'notes'] = 'Price Range expected ' + df_tk['price_range_expected_date'].astype(str)
        df_tk.loc[df_tk['price_range_expected_date'].notna(), 'price_range'] = np.nan
        df_tk.loc[df_tk['price_expected_date'].notna(), 'notes'] = 'Price expected ' + df_tk['price_expected_date'].astype(str)
        df_tk.sort_values('time_checked', inplace=True)
        df_tk.sort_values('price', inplace=True)
        df_tk.drop_duplicates(subset=['ticker'], inplace=True)

        df = pd.merge(df_jp.drop(columns=['time_added']),
                      df_tk[['ticker', 'ipo_date', 'price', 'price_range', 'notes', 'time_added']], how='left',
                      on=['ticker', 'ipo_date'], suffixes=('_jp', '_tk'))
        df['notes'] = df['notes_tk'].fillna('') + df['notes_jp'].fillna('')
        df['ticker'] = df['ticker'].astype(str)
        df.sort_values(by='time_checked', ascending=False, inplace=True)
        df.drop_duplicates()
        self.append_to_all(df)

    def cn(self):

        def shanghai():
            file_name = 'Shanghai'
            assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
            df = self.src_dfs.get(file_name).copy()
            df = self.format_date_cols(df, ['subscription_date', 'announcement_of_winning_results', 'ipo_date',
                                            'time_checked'])
            df['company_name'] = df['new_share_name'].str.extract(r'^(\w*)\s')
            df['ticker'] = df['company_name'].str.extract(r'\w(\d*)\b')
            df['ticker'] = df['ticker'].astype(str)
            df['company_name'] = df['company_name'].str.replace(r'\w(\d*)\b', '', regex=True)
            df['exchange'] = 'Shanghai Stock Exchange'
            return df

        def cninfo():
            file_name = 'CNInfo'
            assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
            df = self.src_dfs.get(file_name).copy()
            df = self.format_date_cols(df, ['ipo_date', 'release_date', 'time_checked'])
            df['exchange'] = 'Shenzhen Stock Exchange'
            df['ticker'] = df['ticker'].astype(str)
            df['shares_offered'] = df['shares_offered'].astype(int, errors='ignore') * 10000
            return df

        def eastmoney():
            file_name = 'East Money'
            assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
            df = self.src_dfs.get(file_name).copy()
            df.replace('-', np.nan, inplace=True)
            df = self.format_date_cols(df, ['time_checked'])
            df['ticker'] = df['ticker'].astype(str)
            # date is provided as mm-dd, adding current year to make the date formatted as mm-dd-yyyy
            df['ipo_date'] = df['ipo_date'] + f"-{datetime.now().year}"
            df['ipo_date'] = pd.to_datetime(df['ipo_date'], errors='coerce').dt.date
            # at the beginning of the year, the calendar will still show IPOs from last year
            # adding the current year to that previous date will be incorrect
            # those incorrect dates will be 6+ months away, we shouldn't see legitimate IPO dates that far in advance
            # if the IPO date is more than 6 months away, I subtract 1 year from the IPO date
            df.loc[df['ipo_date'] > (pd.to_datetime('today') + pd.offsets.DateOffset(months=6)), 'ipo_date'] = df['ipo_date'] - pd.offsets.DateOffset(years=1)
            return df

        df_sh = shanghai()
        df_sz = cninfo()
        df = pd.concat([df_sh, df_sz], ignore_index=True, sort=False)
        df_em = eastmoney()
        df_em.sort_values(by=['time_checked'], ascending=False, inplace=True)
        df_em.drop_duplicates(subset=['ticker'], inplace=True)
        df = pd.merge(df, df_em[['ticker', 'ipo_date']], how='left', on='ticker', suffixes=('_exch', '_em'))
        df['ipo_date'] = df['ipo_date_exch'].fillna(df['ipo_date_em'])
        df.sort_values(by=['time_checked'], ascending=False, inplace=True)
        df.drop_duplicates(subset=['company_name', 'ticker'], inplace=True)
        self.append_to_all(df)

    def euronext(self):
        file_name = 'Euronext'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['ipo_date'], dayfirst=True)
        df = self.format_date_cols(df, ['time_checked'])
        df['exchange'] = df['exchange'] + ' ' + df['location']
        df.rename(columns={'isin': 'ticker'}, inplace=True)
        self.append_to_all(df)

    def aastocks(self):
        file_name = 'AAStocks'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_checked'])
        df['exchange'] = 'Hong Kong Stock Exchange'
        df['ticker'] = df['ticker'].str.extract(r'(\d*)\.HK')
        # TODO: AAStocks might show max offer price as the price?
        df.loc[df['price'].str.contains('-', na=False), 'price_range'] = df['price']
        df.loc[df['price'].str.contains('-', na=False), 'price'] = np.nan
        self.append_to_all(df)

    def lse(self):
        file_name = 'LSE'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_checked'])
        df['company_name'] = df['company_name'].str.replace(r'\s\(.*\)', '', regex=True)
        df['exchange'] = 'London Stock Exchange ' + df['exchange'].fillna('')
        self.append_to_all(df)

    def ca(self):

        def bs():
            file_name = 'BS-TSX'
            assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
            bstsx = self.src_dfs.get(file_name).copy()
            bstsx['exchange'] = 'TSX'

            file_name = 'BS-TSXV'
            assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
            bstsxv = self.src_dfs.get(file_name).copy()
            bstsxv['exchange'] = 'TSX Venture'

            df_bs = pd.concat([bstsx, bstsxv], ignore_index=True)
            df_bs = self.format_date_cols(df_bs, ['ipo_date', 'time_checked'])
            df_bs.loc[df_bs['company_name'].str.contains(' ETF', na=False), 'security_type'] = 'ETF'
            df_bs.loc[df_bs['company_name'].str.contains(' Fixed Income', na=False), 'security_type'] = 'Fixed Income'
            df_bs.loc[df_bs['company_name'].str.contains(' Private Pool', na=False), 'security_type'] = 'Private Pool'
            df_bs = df_bs.loc[df_bs['security_type'].isna()]
            return df_bs

        def tsx():
            file_name = 'TSX'
            assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
            df_tsx = self.src_dfs.get(file_name).copy()
            df_tsx = self.format_date_cols(df_tsx, ['ipo_date', 'time_checked'])
            df_tsx['ticker'] = df_tsx['company_name'].str.extract(r'\(([a-zA-Z\.,\s]*)\)')
            df_tsx['company_name'] = df_tsx['company_name'].str.extract(r'^([a-zA-Z\.\s\d&,\-]*)[\xa0|\(\+]')
            df_tsx['company_name'] = df_tsx['company_name'].str.strip()
            df_tsx['exchange'] = 'TSX'
            df_tsx.loc[df_tsx['company_name'].str.contains(' ETF', na=False), 'security_type'] = 'ETF'
            df_tsx = df_tsx.loc[df_tsx['security_type'].isna()]
            df_tsx.rename(columns={'ipo_date': 'IPO Date'}, inplace=True)
            return df_tsx

        df_b = bs()
        df_t = tsx()
        df = pd.concat([df_b, df_t], join='outer', ignore_index=True).drop_duplicates(subset=['company_name'])
        self.append_to_all(df)

    def frankfurt(self):
        file_name = 'Frankfurt'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_checked'])
        rows_to_shift = df.loc[(df['first_price_and_market_cap'].isna()) &
                               (~df['sub_price_and_deal_size'].isna())].index.to_list()
        df.iloc[rows_to_shift, 4:] = df.iloc[rows_to_shift, 4:].shift(1, axis=1)
        df['company_name'] = df['summary'].str.extract(r'\)([a-zA-Z\s\d\-&\.,]*)Sector')
        df['offer_type'] = df['market_segment'].str.extract(r'\(([a-zA-Z\s\/]*)\)')
        df = df.loc[df['offer_type'] != 'Transfer']
        df['notes'] = 'Offer Type: ' + df['offer_type'].fillna('')
        df['exchange'] = 'Frankfurt Stock Exchange - ' + df['market_segment'].str.extract(r'^([a-zA-Z\s]*)\s\(')
        df['first_traded_price'] = df['first_price_and_market_cap'].str.extract(r'Quotation: €\s(\d{1,3}\.\d{1,3})')
        df['market_cap'] = df['first_price_and_market_cap'].str.extract(r'/\s€\s([\d,\.]*)')
        df['price'] = df['sub_price_and_deal_size'].str.extract(r'Volume: €\s(\d{1,3}\.\d{1,3})')
        df['deal_size'] = df['sub_price_and_deal_size'].str.extract(r'/\s€\s([\d,\.]*)')
        df['sector'] = df['sector'].str.extract(r'Sector:\n\t\t\t([a-zA-|&,\.\s]*)')
        self.append_to_all(df)

    def krx(self):
        file_name = 'KRX'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_checked'])
        df['ticker'] = df['ticker'].astype(str)
        df['exchange'] = 'Korea Exchange'
        for c in ['shares_outstanding', 'par_value', 'price']:
            df[c] = df[c].str.replace(',', '').astype(int, errors='ignore')
        self.append_to_all(df)

    def asx(self):
        file_name = 'ASX'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        self.append_to_all(df)

    def twse(self):
        file_name = 'TWSE'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['announcement_date', 'listing_review_date', 'application_approval_date',
                                        'listing_agreement_submitted_to_fsc_date', 'ipo_date', 'time_checked'])
        df['ticker'] = df['ticker'].astype(str)
        df['exchange'] = 'Taiwan Stock Exchange'

        self.append_to_all(df)

    def bme(self):
        file_name = 'BME'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_checked'])
        for c in ['shares_offered', 'deal_size', 'volume']:
            df[c] = df[c].str.replace(',', '').astype(float).astype(int)
        df['price'] = df['volume'] / df['shares_offered']
        df['exchange'] = 'Bolsa de Madrid'
        df = df.loc[df['listing_type'] != 'Integration']
        df.rename(columns={'isin': 'ticker'}, inplace=True)
        self.append_to_all(df)

    def sgx(self):
        file_name = 'SGX'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_checked'])
        df['exchange'] = 'Singapore Exchange - ' + df['market_segment'].fillna('')
        df['price'] = df['price'].str.extract(r'\s([\d\.]*)$')
        df.drop(df.loc[df['company_name'].str.contains(' ETF', na=False)].index, inplace=True)
        df.drop(columns=['closing_price_first_day', 'change_from_ipo_price', 'closing_price_prev_day',
                         'market_cap_prev_day_mm', 'prem_disc_to_ipo_price'], inplace=True)
        self.append_to_all(df)

    def idx(self):
        file_name = 'IDX'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'delisting_date', 'time_checked'])
        df['exchange'] = 'Indonesia Stock Exchange - ' + df['market_segment'].fillna('')
        self.append_to_all(df)

    def bm(self):
        file_name = 'BM'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['subscription_date_start', 'subscription_date_end', 'ipo_date', 'time_checked'])
        df['exchange'] = 'Bursa Malaysia - ' + df['market_segment'].fillna('')
        df['price'] = df['price'].str.extract(r'(\d*\.\d*)')
        self.append_to_all(df)

    def nasdaqnordic(self):
        file_name = 'NasdaqNordic'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_checked'])
        df['exchange'] = 'Nasdaq Nordic'
        # Nasdaq Nordic comes with columns for last price and Percent Change change in price which change every day
        df.drop(columns=['last_price', 'percent_change'], inplace=True)
        df.sort_values(by='time_checked', inplace=True)
        df.drop_duplicates(inplace=True)
        self.append_to_all(df)

    def spotlight(self):
        file_name = 'SpotlightAPI'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_checked'])
        df['exchange'] = 'Spotlight'
        self.append_to_all(df)

    def italy(self):
        file_name = 'BIT'
        assert file_name in self.src_dfs.keys(), f"No CSV file for {file_name} in Source Data folder."
        df = self.src_dfs.get(file_name).copy()
        df = self.format_date_cols(df, ['ipo_date'], dayfirst=True)
        df = self.format_date_cols(df, ['time_checked'])
        df['market_segment'] = df['market_segment'].str.replace('*', 'Professional Segment', regex=False)
        df['exchange'] = 'Borsa Italiana - ' + df['market_segment']
        df = df.loc[~df['listing_type'].str.contains('Transition from')]
        self.append_to_all(df)

    def formatting_all(self):
        # removing commas from company name - Concordance API will interpret those as new columns
        self.df_all['company_name'] = self.df_all['company_name'].str.replace(',', '', regex=False)
        self.df_all = self.format_date_cols(self.df_all, ['ipo_date', 'time_checked'])
        self.df_all.loc[self.df_all['ipo_date'].dt.date > date.today(), 'status'] = 'Upcoming ' + self.df_all['status'].fillna('')
        self.df_all.loc[self.df_all['ipo_date'].dt.date == date.today(), 'status'] = 'Listing Today'
        self.df_all['status'] = self.df_all['status'].str.strip()
        self.df_all['ipo_date'] = self.df_all['ipo_date'].dt.strftime('%Y-%m-%d')
        self.df_all['price'] = pd.to_numeric(self.df_all['price'], errors='coerce')
        self.df_all.sort_values(by='time_checked', ascending=False, inplace=True)
        self.df_all.drop_duplicates(subset=['company_name', 'exchange'], inplace=True)
        self.df_all.sort_values(by=['ipo_date', 'time_checked'], ascending=False, inplace=True)
        self.df_all.reset_index(drop=True, inplace=True)

    def save_all(self):
        # TODO: keep db friendly names once ready to save other files to db
        # renaming back to original names for now
        self.df_all.rename(columns={
            'company_name': 'Company Name',
            'ticker': 'Symbol',
            'exchange': 'Market',
            'ipo_date': 'IPO Date',
            'price': 'Price',
            'price_range': 'Price Range',
            'status': 'Status',
            'notes': 'Notes'
        }, inplace=True)
        self.df_all.to_excel(self.result_file, sheet_name='All IPOs', index=False, encoding='utf-8-sig',
                             freeze_panes=(1, 0))


def main():
    logger.info("Combining all the data from external sources together")
    dt = DataTransformation()
    try:
        dt.us()
        dt.jpx()
        dt.cn()
        dt.euronext()
        dt.aastocks()
        dt.lse()
        dt.ca()
        dt.frankfurt()
        dt.krx()
        dt.asx()
        dt.twse()
        dt.bme()
        dt.sgx()
        dt.idx()
        dt.bm()
        dt.nasdaqnordic()
        dt.spotlight()
        dt.italy()
    except Exception as e:
        logger.error(e, exc_info=sys.exc_info())
        error_email(str(e))
    finally:
        dt.formatting_all()
        dt.save_all()


if __name__ == '__main__':
    main()
