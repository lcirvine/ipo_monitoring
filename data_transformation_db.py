import os
import sys
import pandas as pd
import numpy as np
from datetime import date, datetime
from logging_ipo_dates import logger, error_email
import json
from pg_connection import pg_connection, sql_types

pd.options.mode.chained_assignment = None


class DataTransformation:
    def __init__(self):
        self.time_checked_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M')
        self.conn = pg_connection()
        self.source_folder = os.path.join(os.getcwd(), 'Data from Sources')
        self.final_cols = ['company_name', 'ticker', 'exchange', 'ipo_date', 'price', 'price_range', 'shares_offered',
                           'status', 'notes', 'time_added', 'time_removed']
        self.df_all = pd.DataFrame(columns=self.final_cols)
        self.result_folder = os.path.join(os.getcwd(), 'Results')
        if not os.path.exists(self.result_folder):
            os.mkdir(self.result_folder)
        self.result_file = os.path.join(self.result_folder, 'All IPOs.xlsx')
        sources_file = os.path.join(os.getcwd(), 'sources.json')
        if os.path.exists(sources_file):
            with open(sources_file, 'r') as f:
                self.sources = json.load(f)
        self.src_dfs = self.all_source_files()

    def all_source_files(self):
        return {src: pd.read_sql_table(v['db_table_raw'], self.conn) for src, v in self.sources.items()}

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
            if c in df.columns and df[c].dtype.name != 'datetime64[ns]':
                df[c] = pd.to_datetime(df[c], errors='coerce', dayfirst=dayfirst)
        return df

    def us(self):

        def nyse():
            source_name = 'NYSE'
            assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
            df_up = self.src_dfs.get(source_name).copy()
            df_up = self.format_date_cols(df_up, ['ipo_date', 'time_added'])
            # NYSE provides the expected pricing date, the expected listing date is one day after
            df_up['ipo_date'] = df_up['ipo_date'] + pd.offsets.DateOffset(days=1)
            df_up['shares_offered'] = df_up['shares_offered'].str.replace(',', '').astype(int, errors='ignore')
            df_up['deal_size'] = df_up['deal_size'].str.replace(',', '').astype(float, errors='ignore')
            tbl = self.sources[source_name]['db_table']
            df_up.to_sql(tbl, self.conn, if_exists='replace', index=False,
                         dtype={
                             'ipo_date': sql_types.Date,
                             'time_added': sql_types.DateTime,
                             'time_removed': sql_types.DateTime,
                             'shares_offered': sql_types.Float,
                             'deal_size': sql_types.Float
                         })

            source_name = 'NYSE Withdrawn'
            assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
            df_wd = self.src_dfs.get(source_name).copy()
            df_wd = self.format_date_cols(df_wd, ['postponement_date', 'time_added'])
            df_wd['notes'] = 'Withdrawn on ' + df_wd['postponement_date'].astype(str)
            df_wd['exchange'] = 'NYSE'
            df_wd['shares_offered'] = df_wd['shares_offered'].str.replace(',', '').astype(int, errors='ignore')
            tbl = self.sources[source_name]['db_table']
            df_wd.to_sql(tbl, self.conn, if_exists='replace', index=False,
                         dtype={
                             'postponement_date': sql_types.Date,
                             'time_added': sql_types.DateTime,
                             'time_removed': sql_types.DateTime,
                             'shares_offered': sql_types.Float
                         })

            df = pd.concat([df_up, df_wd], ignore_index=True, sort=False)
            return df

        def nasdaq():
            source_name = 'Nasdaq'
            assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
            df_up = self.src_dfs.get(source_name).copy()
            df_up = self.format_date_cols(df_up, ['ipo_date', 'time_added'])
            df_up.loc[df_up['price'].str.contains('-', na=False), 'price_range'] = df_up['price']
            df_up.loc[df_up['price'].str.contains('-', na=False), 'price'] = np.nan
            df_up['price'] = df_up['price'].str.replace(' ', '')
            df_up['price'] = pd.to_numeric(df_up['price'], errors='coerce')
            df_up['shares_offered'] = df_up['shares_offered'].str.replace(',', '').astype(int, errors='ignore')
            tbl = self.sources[source_name]['db_table']
            df_up.to_sql(tbl, self.conn, if_exists='replace', index=False,
                         dtype={
                             'ipo_date': sql_types.Date,
                             'time_added': sql_types.DateTime,
                             'time_removed': sql_types.DateTime,
                             'shares_offered': sql_types.Float,
                             'price': sql_types.Float
                         })

            source_name = 'Nasdaq Priced'
            assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
            df_p = self.src_dfs.get(source_name).copy()
            df_p = self.format_date_cols(df_p, ['ipo_date', 'time_added'])
            df_p['shares_offered'] = df_p['shares_offered'].str.replace(',', '').astype(int, errors='ignore')
            tbl = self.sources[source_name]['db_table']
            df_p.to_sql(tbl, self.conn, if_exists='replace', index=False,
                        dtype={
                            'ipo_date': sql_types.Date,
                            'time_added': sql_types.DateTime,
                            'time_removed': sql_types.DateTime,
                            'shares_offered': sql_types.Float
                        })

            source_name = 'Nasdaq Withdrawn'
            assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
            df_wd = self.src_dfs.get(source_name).copy()
            df_wd = self.format_date_cols(df_wd, ['announcement_date', 'cancellation_date', 'time_added'])
            df_wd['notes'] = 'Withdrawn on ' + df_wd['cancellation_date'].astype(str)
            df_wd['status'] = 'Withdrawn'
            df_wd['exchange'] = 'Nasdaq'
            df_wd['shares_offered'] = df_wd['shares_offered'].str.replace(',', '').astype(int, errors='ignore')
            tbl = self.sources[source_name]['db_table']
            df_wd.to_sql(tbl, self.conn, if_exists='replace', index=False,
                         dtype={
                             'announcement_date': sql_types.Date,
                             'cancellation_date': sql_types.Date,
                             'time_added': sql_types.DateTime,
                             'time_removed': sql_types.DateTime,
                             'shares_offered': sql_types.Float
                         })

            df = pd.concat([df_up, df_p, df_wd], ignore_index=True, sort=False)
            return df

        def iposcoop():
            source_name = 'IPOScoop'
            assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
            df = self.src_dfs.get(source_name).copy()
            df['status'] = df['ipo_date'].str.extract(r'(Priced|Postponed)')
            # dropping 'week of' dates because they're just not accurate enough
            # df['notes'] = df['ipo_date'].str.extract(r'(Week of)')
            df = df.loc[~df['ipo_date'].str.contains('Week of')]
            df['ipo_date'] = df['ipo_date'].str.extract(r'(\d{1,2}/\d{1,2}/\d{4})')
            df = self.format_date_cols(df, ['ipo_date', 'time_added'])
            df.loc[df['price_range_low'] != df['price_range_high'], 'price_range'] = df['price_range_low'].astype(
                str) + ' - ' + df['price_range_high'].astype(str)
            df.loc[df['price_range_low'] == df['price_range_high'], 'price'] = df['price_range_high']
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            df['exchange'] = 'IPOScoop'
            df['shares_offered'] = pd.to_numeric(df['shares_offered_mm'], errors='ignore')
            df['shares_offered'] = df['shares_offered'] * 1000000
            tbl = self.sources[source_name]['db_table']
            df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                      dtype={
                          'ipo_date': sql_types.Date,
                          'time_added': sql_types.DateTime,
                          'time_removed': sql_types.DateTime,
                          'shares_offered': sql_types.Float,
                          'price': sql_types.Float
                      })
            return df

        def av():
            source_name = 'AlphaVantage'
            assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
            df = self.src_dfs.get(source_name).copy()
            df = self.format_date_cols(df, ['ipo_date', 'time_added'])
            df.loc[(df['price_range_low'] != df['price_range_high']) &
                   (df['price_range_low'] != 0), 'price_range'] = df['price_range_low'].astype(str) + ' - ' + df[
                'price_range_high'].astype(str)
            df.loc[(df['price_range_low'] == df['price_range_high']) & (df['price_range_low'] != 0), 'price'] = df[
                'price_range_high']
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            # exchange names sometimes have triple quotes around them i.e. """NYSE American"""
            df['exchange'] = df['exchange'].str.replace('"', '')
            # assetTypes given are Units, Shares, Warrants, Rights - adding ETFs
            df.loc[df['company_name'].str.contains(' ETF'), 'assetType'] = 'ETF'
            # Warrants, Rights, and ETFs will have price_range_low and price_range_high = 0
            # Shares where price_range_low and price_range_high = 0 are either direct listings or splitting SPAC units
            # saving all assetTypes (including warrants, rights, ETFs) because symbology would like to have them
            # removing asset types from company name
            asset_text = [' ETF Trust', ' ETF', ' Units', ' Unit', ' Warrants to purchase ', ' Warrants', ' Warrant', ' Rights'
                          ' Class A Common Stock', ' Class A Ordinary Shares', ' Class A Ordinary Share',
                          ' Subordinate Voting Shares', ' American Depository Shares', ' American Depositary Shares',
                          ' Common Stock', ' Common Shares', ' Ordinary Shares']
            for att in asset_text:
                df['company_name'] = df['company_name'].str.replace(att, '', case=False, regex=False)
            tbl = self.sources[source_name]['db_table']
            df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                      dtype={
                          'ipo_date': sql_types.Date,
                          'time_added': sql_types.DateTime,
                          'time_removed': sql_types.DateTime,
                          'price': sql_types.Float
                      })
            return df

        df_ny = nyse()
        df_nd = nasdaq()
        df_is = iposcoop()
        df_av = av()
        df = pd.concat([df_nd, df_ny], ignore_index=True, sort=False)

        # Add iposcoop and av only when there is not already a row from one of the exchanges
        # company names are often different so joining on symbol after removing special characters
        special_chars = r"(\.|'|\*)"
        df['formatted symbol'] = df['ticker'].str.replace(special_chars, "", regex=True)

        def add_only_new(data_frame: pd.DataFrame):
            data_frame['formatted symbol'] = data_frame['ticker'].str.replace(special_chars, "", regex=True)
            data_frame = pd.merge(data_frame, df, how='outer', on='formatted symbol', suffixes=('', '_drop'),
                                  indicator=True)
            data_frame = data_frame.loc[data_frame['_merge'] == 'left_only']
            drop_cols = [c for c in data_frame.columns if '_drop' in c]
            drop_cols.append('_merge')
            data_frame.drop(columns=drop_cols, inplace=True)
            return data_frame

        df_is = add_only_new(df_is)
        df = pd.concat([df, df_is], ignore_index=True, sort=False)
        df_av = add_only_new(df_av)
        df = pd.concat([df, df_av], ignore_index=True, sort=False)

        df.sort_values(by=['time_added'], ascending=False, inplace=True)
        df.drop_duplicates(subset=['company_name', 'ticker'], inplace=True)
        df.loc[df['ipo_date'] >= pd.to_datetime('today'), 'notes'] = 'Price expected ' + (
                df['ipo_date'] - pd.offsets.DateOffset(days=1)).astype(str)
        self.append_to_all(df)

    def jpx(self):
        source_name = 'JPX'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df_jp = self.src_dfs.get(source_name).copy()
        df_jp = self.format_date_cols(df_jp, ['ipo_date', 'date_of_listing_approval', 'time_added'])
        df_jp['exchange'] = 'Japan Stock Exchange - ' + df_jp['market_segment']
        df_jp.loc[df_jp['company_name'].str.contains(r'\*\*', regex=True), 'notes'] = 'Technical Listing'
        df_jp['company_name'] = df_jp['company_name'].str.replace(r',', ', ')
        df_jp['company_name'] = df_jp['company_name'].str.replace(r'\*\*', '', regex=True)
        df_jp['company_name'] = df_jp['company_name'].str.strip()
        tbl = self.sources[source_name]['db_table']
        df_jp.to_sql(tbl, self.conn, if_exists='replace', index=False,
                     dtype={
                         'ipo_date': sql_types.Date,
                         'date_of_listing_approval': sql_types.Date,
                         'time_added': sql_types.DateTime,
                         'time_removed': sql_types.DateTime
                     })

        source_name = 'TokyoIPO'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df_tk = self.src_dfs.get(source_name).copy()
        df_tk = self.format_date_cols(df_tk, ['ipo_date', 'time_added'])
        df_tk.loc[df_tk['price_range_expected_date'].notna(), 'notes'] = 'Price Range expected ' + df_tk[
            'price_range_expected_date'].astype(str)
        df_tk.loc[df_tk['price_range_expected_date'].notna(), 'price_range'] = np.nan
        df_tk.loc[df_tk['price_expected_date'].notna(), 'notes'] = 'Price expected ' + df_tk[
            'price_expected_date'].astype(str)
        df_tk['price'] = pd.to_numeric(df_tk['price'], errors='coerce')
        df_tk.sort_values('time_added', inplace=True)
        df_tk.sort_values('price', inplace=True)
        df_tk.drop_duplicates(subset=['ticker'], inplace=True)
        tbl = self.sources[source_name]['db_table']
        df_tk.to_sql(tbl, self.conn, if_exists='replace', index=False,
                     dtype={
                         'ipo_date': sql_types.Date,
                         'time_added': sql_types.DateTime,
                         'time_removed': sql_types.DateTime,
                         'price': sql_types.Float
                     })

        df = pd.merge(df_jp.drop(columns=['time_added']),
                      df_tk[['ticker', 'ipo_date', 'price', 'price_range', 'notes', 'time_added']], how='left',
                      on=['ticker', 'ipo_date'], suffixes=('_jp', '_tk'))
        df['notes'] = df['notes_tk'].fillna('') + df['notes_jp'].fillna('')
        df['ticker'] = df['ticker'].astype(str)
        df.sort_values(by=['time_added'], ascending=False, inplace=True)
        df.drop_duplicates()
        self.append_to_all(df)

    def cn(self):

        def shanghai():
            source_name = 'Shanghai'
            assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
            df = self.src_dfs.get(source_name).copy()
            df = self.format_date_cols(df, ['subscription_date', 'announcement_of_winning_results', 'ipo_date',
                                            'time_added'])
            df['company_name'] = df['new_share_name'].str.replace(r'(\d*)', '', regex=True)
            df['ticker'] = df['new_share_name'].str.extract(r'(\d*)$')
            df['ticker'] = df['ticker'].astype(str)
            df['exchange'] = 'Shanghai Stock Exchange'
            tbl = self.sources[source_name]['db_table']
            df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                      dtype={
                          'subscription_date': sql_types.Date,
                          'announcement_of_winning_results': sql_types.Date,
                          'ipo_date': sql_types.Date,
                          'time_added': sql_types.DateTime,
                          'time_removed': sql_types.DateTime
                      })
            return df

        def cninfo():
            source_name = 'CNInfo'
            assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
            df = self.src_dfs.get(source_name).copy()
            df = self.format_date_cols(df, ['ipo_date', 'release_date', 'time_added'])
            df['exchange'] = 'Shenzhen Stock Exchange'
            df['ticker'] = df['ticker'].astype(str)
            df['shares_offered'] = pd.to_numeric(df['shares_offered'], errors='coerce')
            df['shares_offered'] = df['shares_offered'] * 10000
            tbl = self.sources[source_name]['db_table']
            df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                      dtype={
                          'ipo_date': sql_types.Date,
                          'release_date': sql_types.Date,
                          'time_added': sql_types.DateTime,
                          'time_removed': sql_types.DateTime,
                          'shares_offered': sql_types.Float
                      })
            return df

        def eastmoney():
            source_name = 'East Money'
            assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
            df = self.src_dfs.get(source_name).copy()
            df.replace('-', np.nan, inplace=True)
            df = self.format_date_cols(df, ['time_added'])
            df['ticker'] = df['ticker'].astype(str)
            # date is provided as mm-dd, adding current year to make the date formatted as mm-dd-yyyy
            df['ipo_date'] = df['ipo_date'] + f"-{datetime.now().year}"
            df['ipo_date'] = pd.to_datetime(df['ipo_date'], errors='coerce')
            # at the beginning of the year, the calendar will still show IPOs from last year
            # adding the current year to that previous date will be incorrect
            # those incorrect dates will be 6+ months away, we shouldn't see legitimate IPO dates that far in advance
            # if the IPO date is more than 6 months away, I subtract 1 year from the IPO date
            df.loc[df['ipo_date'] > (pd.to_datetime('today') + pd.offsets.DateOffset(months=6)), 'ipo_date'] = df['ipo_date'] - pd.offsets.DateOffset(years=1)
            tbl = self.sources[source_name]['db_table']
            df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                      dtype={
                          'ipo_date': sql_types.Date,
                          'time_added': sql_types.DateTime,
                          'time_removed': sql_types.DateTime
                      })
            return df

        df_sh = shanghai()
        df_sz = cninfo()
        df = pd.concat([df_sh, df_sz], ignore_index=True, sort=False)
        df_em = eastmoney()
        df_em.sort_values(by=['time_added'], ascending=False, inplace=True)
        df_em.drop_duplicates(subset=['ticker'], inplace=True)
        df_em = df_em.loc[df_em['ipo_date'].notna(), ['ticker', 'ipo_date']]
        df = pd.merge(df, df_em[['ticker', 'ipo_date']], how='left', on='ticker', suffixes=('_exch', '_em'))
        df['ipo_date'] = df['ipo_date_exch'].fillna(df['ipo_date_em'].fillna(pd.NaT))
        df.sort_values(by=['time_added'], ascending=False, inplace=True)
        df.drop_duplicates(subset=['company_name', 'ticker'], inplace=True)
        self.append_to_all(df)

    def euronext(self):
        source_name = 'Euronext'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['ipo_date'], dayfirst=True)
        df = self.format_date_cols(df, ['time_added'])
        df['exchange'] = df['exchange'] + ' ' + df['location']
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime
                  })
        self.append_to_all(df)

    def aastocks(self):
        source_name = 'AAStocks'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_added'])
        df['exchange'] = 'Hong Kong Stock Exchange'
        df['ticker'] = df['ticker'].str.extract(r'(\d*)\.HK')
        # TODO: AAStocks might show max offer price as the price?
        df.loc[df['price'].str.contains('-', na=False), 'price_range'] = df['price']
        df.loc[df['price'].str.contains('-', na=False), 'price'] = np.nan
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime,
                      'price': sql_types.Float
                  })
        self.append_to_all(df)

    def lse(self):
        source_name = 'LSE'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_added'])
        df['company_name'] = df['company_name'].str.replace(r'\s\(.*\)', '', regex=True)
        df['exchange'] = 'London Stock Exchange ' + df['exchange'].fillna('')
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime
                  })
        self.append_to_all(df)

    def tmx(self):
        source_name = 'TMX'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df.rename(columns={'list_symbol': 'ticker', 'effective_date': 'ipo_date', 'details': 'notes'}, inplace=True)
        # notes (renamed from details) can be really long, truncating to 200 characters
        df['notes'] = df['notes'].str[:200]
        latest = df.loc[(df['file'].str[:8] == df['file'].max()[:8]), 'identification'].to_list()
        df.loc[(~df['identification'].isin(latest) & df['time_removed'].isna()), 'time_removed'] = self.time_checked_str
        df.sort_values(by=['entry_date', 'file', 'time_added'], inplace=True)
        ss = [col for col in df.columns if col not in ('time_added', 'file')]
        df.drop_duplicates(subset=ss, inplace=True)
        df = df.loc[
            (df['change_type'] == 'New Listing') &
            (~df['notes'].str.contains('transfer', na=False, case=False)) &
            (~df['company_name'].str.contains(' ETF| Fund', na=False)) &
            (~df['security_description'].str.contains('Debentures|Warrants|Rights|Common share purchase warrants', na=False))
        ]
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'entry_date': sql_types.Date,
                      'modification_date': sql_types.Date,
                      'identification': sql_types.Integer,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime
                  })
        self.append_to_all(df)

    def frankfurt(self):
        source_name = 'Frankfurt'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_added'])
        df.fillna(np.nan, inplace=True)
        rows_to_shift = df.loc[df['sub_price_and_deal_size'].str.startswith('First Price', na=False)].index.to_list()
        df.loc[rows_to_shift, 'first_price_and_market_cap'] = df['sub_price_and_deal_size']
        df.loc[rows_to_shift, 'sub_price_and_deal_size'] = np.nan
        df['company_name'] = df['summary'].str.extract(r'\)([a-zA-Z\s\d\-&\.,]*)Sector')
        df['offer_type'] = df['market_segment'].str.extract(r'\(([a-zA-Z\s\/]*)\)')
        df = df.loc[df['offer_type'] != 'Transfer']
        df['notes'] = 'Offer Type: ' + df['offer_type'].fillna('')
        df['exchange'] = 'Frankfurt Stock Exchange - ' + df['market_segment'].str.extract(r'^([a-zA-Z\s]*)\s\(')
        df['first_traded_price'] = df['first_price_and_market_cap'].str.extract(r'Quotation: €\s(\d{1,3}\.\d{1,3})')
        df['market_cap'] = df['first_price_and_market_cap'].str.extract(r'/\s€\s([\d,\.]*)')
        df['price'] = df['sub_price_and_deal_size'].str.extract(r'Volume: €\s(\d{1,3}\.\d{1,3})')
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['deal_size'] = df['sub_price_and_deal_size'].str.extract(r'/\s€\s([\d,\.]*)')
        df['sector'] = df['sector'].str.extract(r'Sector:\n\t\t\t([a-zA-|&,\.\s]*)')
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime,
                      'price': sql_types.Float
                  })
        self.append_to_all(df)

    def krx(self):
        source_name = 'KRX'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_added'])
        df['ticker'] = df['ticker'].astype(str)
        df['exchange'] = 'Korea Exchange'
        for c in ['shares_outstanding', 'par_value', 'price']:
            df[c] = df[c].str.replace(',', '').astype(int, errors='ignore')
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime,
                      'shares_outstanding': sql_types.Float,
                      'par_value': sql_types.INTEGER,
                      'price': sql_types.Float
                  })
        self.append_to_all(df)

    def asx(self):
        source_name = 'ASX'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df.sort_values(by=['time_added'], inplace=True)
        df.drop_duplicates(subset=[col for col in df.columns if 'time' not in col], inplace=True)
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime
                  })
        self.append_to_all(df)

    def twse(self):
        source_name = 'TWSE'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['announcement_date', 'listing_review_date', 'application_approval_date',
                                        'listing_agreement_submitted_to_fsc_date', 'ipo_date', 'time_added'])
        df['ticker'] = df['ticker'].astype(str)
        df['exchange'] = 'Taiwan Stock Exchange'
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime,
                      'announcement_date': sql_types.Date,
                      'listing_review_date': sql_types.Date,
                      'application_approval_date': sql_types.Date,
                      'listing_agreement_submitted_to_fsc_date': sql_types.Date
                  })
        self.append_to_all(df)

    def bme(self):
        source_name = 'BME'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_added'])
        for c in ['shares_offered', 'deal_size', 'volume']:
            df[c] = df[c].str.replace(',', '').astype(float).astype(int)
        df['price'] = df['volume'] / df['shares_offered']
        df['exchange'] = 'Bolsa de Madrid'
        df = df.loc[df['listing_type'] != 'Integration']
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime,
                      'shares_offered': sql_types.Float,
                      'deal_size': sql_types.Float,
                      'volume': sql_types.Float,
                      'price': sql_types.Float
                  })
        df.rename(columns={'isin': 'ticker'}, inplace=True)
        self.append_to_all(df)

    def sgx(self):
        source_name = 'SGX'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_added'])
        df['exchange'] = 'Singapore Exchange - ' + df['market_segment'].fillna('')
        df['price'] = df['price'].str.extract(r'\s([\d\.]*)$')
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df.drop(df.loc[df['company_name'].str.contains(' ETF', na=False)].index, inplace=True)
        df.drop(columns=['closing_price_first_day', 'change_from_ipo_price', 'closing_price_prev_day',
                         'market_cap_prev_day_mm', 'prem_disc_to_ipo_price'], inplace=True)
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime,
                      'price': sql_types.Float
                  })
        self.append_to_all(df)

    def idx(self):
        source_name = 'IDX'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'delisting_date', 'time_added'])
        df['exchange'] = 'Indonesia Stock Exchange - ' + df['market_segment'].fillna('')
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime
                  })
        self.append_to_all(df)

    def bm(self):
        source_name = 'BM'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['subscription_date_start', 'subscription_date_end', 'ipo_date', 'time_added'])
        df['exchange'] = 'Bursa Malaysia - ' + df['market_segment'].fillna('')
        df['price'] = df['price'].str.extract(r'(\d*\.\d*)')
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime,
                      'price': sql_types.Float
                  })
        self.append_to_all(df)

    def nasdaqnordic(self):
        source_name = 'NasdaqNordic'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_added'])
        df['exchange'] = 'Nasdaq Nordic'
        # Nasdaq Nordic comes with columns for last price and percent change in price which change every day
        for col in ['last_price', 'percent_change']:
            if col in df.columns:
                df.drop(columns=col, inplace=True)
        df.sort_values(by='time_added', inplace=True)
        df.drop_duplicates(inplace=True)
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime
                  })
        self.append_to_all(df)

    def spotlight(self):
        source_name = 'SpotlightAPI'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_added'])
        df['exchange'] = 'Spotlight'
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime
                  })
        self.append_to_all(df)

    def italy(self):
        source_name = 'BIT'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['ipo_date'], dayfirst=True)
        df = self.format_date_cols(df, ['time_added'])
        df['market_segment'] = df['market_segment'].str.replace('*', 'Professional Segment', regex=False)
        df['exchange'] = 'Borsa Italiana - ' + df['market_segment']
        df = df.loc[~df['listing_type'].str.contains('Transition from')]
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime
                  })
        self.append_to_all(df)

    def ipohub(self):
        source_name = 'IPOHub'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_added'])
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime,
                      'price': sql_types.Float
                  })
        self.append_to_all(df)

    def nse(self):
        source_name = 'NSE'
        assert source_name in self.src_dfs.keys(), f"No source data for {source_name}."
        df = self.src_dfs.get(source_name).copy()
        df = self.format_date_cols(df, ['ipo_date', 'time_added'])
        tbl = self.sources[source_name]['db_table']
        df.to_sql(tbl, self.conn, if_exists='replace', index=False,
                  dtype={
                      'ipo_date': sql_types.Date,
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime,
                  })
        self.append_to_all(df)

    def formatting_all(self):
        # removing commas from company name - Concordance API will interpret those as new columns
        self.df_all['company_name'] = self.df_all['company_name'].str.replace(',', '', regex=False)
        self.df_all = self.format_date_cols(self.df_all, ['ipo_date', 'time_added'])
        self.df_all.loc[self.df_all['ipo_date'].dt.date > date.today(), 'status'] = 'Upcoming ' + self.df_all[
            'status'].fillna('')
        self.df_all.loc[self.df_all['ipo_date'].dt.date == date.today(), 'status'] = 'Listing Today'
        self.df_all['status'] = self.df_all['status'].str.strip()
        self.df_all['price'] = pd.to_numeric(self.df_all['price'], errors='coerce')
        self.df_all.sort_values(by='time_added', ascending=False, inplace=True)
        self.df_all.drop_duplicates(subset=['company_name', 'exchange'], inplace=True)
        self.df_all.sort_values(by=['ipo_date', 'time_added'], ascending=False, inplace=True)
        self.df_all.reset_index(drop=True, inplace=True)

    def save_all(self):
        df_all_file = self.df_all.copy()
        df_all_file.rename(columns={
            'company_name': 'Company Name',
            'ticker': 'Symbol',
            'exchange': 'Market',
            'ipo_date': 'IPO Date',
            'price': 'Price',
            'price_range': 'Price Range',
            'status': 'Status',
            'notes': 'Notes',
            'time_added': 'time_checked'
        }, inplace=True)
        df_all_file.drop(columns=['time_removed'], inplace=True)
        df_all_file.to_excel(self.result_file, sheet_name='All IPOs', index=False, freeze_panes=(1, 0))

    def save_all_db(self):
        self.df_all.to_sql('all_ipos', self.conn, if_exists='replace', index=False,
                           dtype={
                               'ipo_date': sql_types.Date,
                               'time_added': sql_types.DateTime,
                               'time_removed': sql_types.DateTime,
                               'price': sql_types.Float
                           })

    def close_conn(self):
        self.conn.close()


def main():
    logger.info("Updating db tables and combining data")
    dt = DataTransformation()
    try:
        dt.us()
        dt.jpx()
        dt.cn()
        dt.euronext()
        dt.aastocks()
        # dt.lse()
        dt.tmx()
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
        dt.ipohub()
    except Exception as e:
        logger.error(e, exc_info=sys.exc_info())
        error_email(str(e))
    finally:
        dt.formatting_all()
        dt.save_all_db()
        dt.save_all()
        dt.close_conn()


if __name__ == '__main__':
    main()
