import os
import sys
import pyodbc
import pandas as pd
import numpy as np
from datetime import date
from configparser import ConfigParser
from logging_ipo_dates import logger, error_email
from pg_connection import pg_connection, convert_cols_db, sql_types

pd.options.mode.chained_assignment = None


class DataComparison:
    def __init__(self):
        self.config = ConfigParser()
        self.config.read('db_connection.ini')
        self.results_folder = os.path.join(os.getcwd(), 'Results')
        self.ref_folder = os.path.join(os.getcwd(), 'Reference')
        self.conn = pg_connection()
        self.df_pp = self.pipe_data()
        self.df_e = self.entity_data()
        self.df_s = self.source_data()

    def return_db_connection(self, db_name: str):
        return pyodbc.connect(
            f"Driver={self.config.get(db_name, 'Driver')}"
            f"Server={self.config.get(db_name, 'Server')}"
            f"Database={self.config.get(db_name, 'Database')}"
            f"Trusted_Connection={self.config.get(db_name, 'Trusted_Connection')}",
            timeout=3)

    def pipe_data(self):
        df = pd.read_sql_query(self.config.get('query', 'peopipe'), self.return_db_connection('termcond'))
        df.drop_duplicates(inplace=True)
        pp_file = os.path.join(self.ref_folder, 'PEO-PIPE IPO Data.xlsx')
        # TODO: read from sql table rather than file
        if os.path.exists(pp_file):
            df = pd.concat([pd.read_excel(pp_file, dtype={'CUSIP': str}), df], ignore_index=True, sort=False)
        df['exchange'] = df['exchange'].str.strip()
        df['deal_status'] = df['deal_status'].str.strip()
        df.sort_values(by='last_updated_date_utc', ascending=False, inplace=True)
        # numeric tickers could appear as duplicates if the same ticker has been interpreted as numeric and string
        # Also could have duplicates if ticker is initially NA, then later added for the same master deal
        df['ticker'] = df['ticker'].astype(str)
        df.drop_duplicates(subset=['iconum', 'master_deal', 'ticker'], inplace=True)
        df['ticker'] = df['ticker'].replace(['nan', 'None'], np.nan)
        df.to_excel(pp_file, index=False, encoding='utf-8-sig', freeze_panes=(1, 0))
        try:
            df_sql = df.copy()
            df_sql.columns = convert_cols_db(df_sql.columns)
            df_sql.to_sql('peo_pipe', self.conn, if_exists='replace', index=False)
        except Exception as e:
            logger.error(e, exc_info=sys.exc_info())
        return df

    def entity_data(self):
        return pd.read_excel(os.path.join(self.ref_folder, 'Entity Mapping.xlsx'))

    def source_data(self):
        return pd.read_excel(os.path.join(self.results_folder, 'All IPOs.xlsx'))

    def concatenate_ticker_exchange(self):
        self.df_pp.drop_duplicates(inplace=True)
        self.df_pp['ticker'] = self.df_pp['ticker'].replace(['nan', 'None'], np.nan)
        df_tickers = self.df_pp[['iconum', 'ticker']].dropna(subset=['ticker'])
        df_tickers['ticker'] = df_tickers['ticker'].astype(str)
        df_tickers.drop_duplicates(inplace=True)
        df_tickers = df_tickers.groupby('iconum')['ticker'].apply(', '.join).reset_index()
        df_tickers.set_index('iconum', inplace=True)
        df_exch = self.df_pp[['iconum', 'exchange']].dropna(subset=['exchange'])
        df_exch['exchange'] = df_exch['exchange'].str.strip()
        df_exch.drop_duplicates(inplace=True)
        df_exch = df_exch.groupby('iconum')['exchange'].apply(', '.join).reset_index()
        df_exch.set_index('iconum', inplace=True)
        df_te = pd.concat([df_tickers, df_exch], axis=1).reset_index()
        self.df_pp = self.df_pp.merge(df_te, how='left', on='iconum', suffixes=('_drop', ''))

        df_cusip = self.df_pp[['iconum', 'CUSIP']].dropna(subset=['CUSIP'])
        df_cusip.drop_duplicates(inplace=True)
        df_cusip['CUSIP'] = df_cusip['CUSIP'].astype(str)
        df_cusip = df_cusip.groupby('iconum')['CUSIP'].apply(', '.join).reset_index()
        self.df_pp = self.df_pp.merge(df_cusip, how='left', on='iconum', suffixes=('_drop', ''))

        self.df_pp.drop(columns=['ticker_drop', 'exchange_drop', 'CUSIP_drop'], inplace=True)
        self.df_pp = self.df_pp[['iconum', 'CUSIP', 'Company Name', 'master_deal', 'CUSIP', 'client_deal_id', 'ticker',
                                 'exchange', 'Price', 'min_offering_price', 'max_offering_price', 'announcement_date',
                                 'pricing_date', 'trading_date', 'closing_date', 'deal_status',
                                 'last_updated_date_utc']]
        self.df_pp.sort_values('last_updated_date_utc', ascending=False, inplace=True)
        self.df_pp.drop_duplicates(subset=['iconum', 'master_deal'], inplace=True)

    def merge_entity_data(self):
        # merge data from sources with entities data
        df_m = pd.merge(self.df_s, self.df_e[['Company Name', 'entityName', 'iconum']], how='left')
        # Chinese exchanges will have company name in Chinese
        # the Concordance API will rarely return an iconum for a Chinese name
        # If there is no iconum for a company on a Chinese exchange, try to compare PEO-PIPE data based on the Symbol
        cn_exch = ['Shenzhen Stock Exchange', 'Shanghai Stock Exchange', 'Shanghai', 'Shenzhen']
        pp_cn = self.df_pp.loc[(self.df_pp['exchange'].isin(cn_exch)) & (~self.df_pp['ticker'].isna())]
        pp_cn = pp_cn[['iconum', 'Company Name', 'ticker']]
        df_m = pd.merge(df_m, pp_cn, how='left', left_on='Symbol', right_on='ticker', suffixes=('', '_cn'))
        df_m['iconum'].fillna(df_m['iconum_cn'], inplace=True)
        df_m['entityName'].fillna(df_m['Company Name_cn'], inplace=True)
        # for Chinese Exchanges, get the English name from concordance API or PEO-PIPE data if possible
        df_m.loc[(df_m['Market'].isin(cn_exch)) & ~df_m['entityName'].isna(), 'Company Name'] = df_m['entityName']
        df_m.drop(columns=['iconum_cn', 'Company Name_cn', 'ticker'], inplace=True)
        return df_m

    def file_for_rpds(self):
        """
        Creating a file that will be used to create RPDs for Symbology and Fundamentals teams.
        This will have all IPOs both from external sources and collected internally.
        :return:
        """
        pp_cols = ['iconum', 'CUSIP', 'Company Name', 'client_deal_id', 'ticker', 'exchange', 'Price', 'trading_date',
                   'last_updated_date_utc']
        df_outer = pd.merge(self.merge_entity_data(), self.df_pp[pp_cols], how='outer', on='iconum',
                            suffixes=('_external', '_fds'))
        for c in ['IPO Date', 'trading_date']:
            try:
                # putting this into try/except due to this bug https://github.com/pandas-dev/pandas/issues/39882
                # should be fixed in pandas 1.3 due end of May https://github.com/pandas-dev/pandas/milestone/80
                df_outer[c] = pd.to_datetime(df_outer[c].fillna(pd.NaT), errors='coerce')
            except Exception as e:
                # Reindexing only valid with uniquely valued Index objects
                logger.error(f"{c} - {e}")
        df_ipo = df_outer.loc[
            (df_outer['IPO Date'].dt.date >= date.today())
            | (df_outer['trading_date'].dt.date >= date.today())
        ]
        df_ipo.to_excel(os.path.join(self.ref_folder, 'IPO Monitoring Data.xlsx'), index=False, encoding='utf-8-sig')
        df_wd = df_outer.loc[df_outer['Status'] == 'Withdrawn']
        df_wd.to_excel(os.path.join(self.ref_folder, 'Withdrawn IPOs.xlsx'), index=False, encoding='utf-8-sig')

    def compare(self):
        df_m = pd.merge(self.merge_entity_data(), self.df_pp, how='left', on='iconum', suffixes=('_external', '_fds'))
        df_m.drop_duplicates(inplace=True)
        for c in [col for col in df_m.columns if 'date' in col.lower()]:
            # intermittently getting InvalidIndexError: Reindexing only valid with uniquely valued Index objects
            try:
                df_m[c] = pd.to_datetime(df_m[c].fillna(pd.NaT), errors='coerce').dt.date
            except Exception as e:
                logger.error(f"{c} - {e}")
        df_m['IPO Dates Match'] = df_m['IPO Date'] == df_m['trading_date']
        df_m['IPO Prices Match'] = df_m['Price_external'] == df_m['Price_fds']
        df_m.loc[df_m['Price_external'].isna(), 'IPO Prices Match'] = True
        df_m.loc[df_m['IPO Date'].isna(), 'IPO Dates Match'] = True
        df_m = df_m[['IPO Dates Match', 'IPO Prices Match', 'iconum', 'Company Name_external', 'Symbol', 'Market',
                     'IPO Date', 'Price_external', 'Price Range', 'Status', 'Notes', 'time_checked', 'Company Name_fds',
                     'master_deal', 'client_deal_id', 'CUSIP', 'ticker', 'exchange', 'Price_fds', 'min_offering_price',
                     'max_offering_price', 'announcement_date', 'pricing_date', 'trading_date', 'closing_date',
                     'deal_status', 'last_updated_date_utc']]
        df_m.drop_duplicates(inplace=True)
        df_summary = df_m[['Company Name_external', 'iconum', 'master_deal', 'IPO Date', 'Symbol', 'Market',
                           'Price_external', 'IPO Dates Match', 'IPO Prices Match']]
        df_summary.rename(columns={'Company Name_external': 'Company Name', 'Price_external': 'Price'}, inplace=True)
        df_summary.drop_duplicates(inplace=True)
        df_summary = df_summary.loc[df_summary['IPO Date'] >= date.today()]
        df_summary.sort_values('IPO Date', inplace=True)
        with pd.ExcelWriter(os.path.join(self.results_folder, 'IPO Monitoring.xlsx')) as writer:
            df_m.to_excel(writer, sheet_name='Comparison', index=False, encoding='utf-8-sig', freeze_panes=(1, 0))
            self.df_s.to_excel(writer, sheet_name='Upcoming IPOs - External', index=False, encoding='utf-8-sig', freeze_panes=(1, 0))
            self.df_pp.to_excel(writer, sheet_name='PEO-PIPE IPO Data', index=False, encoding='utf-8-sig', freeze_panes=(1, 0))
            df_summary.to_excel(writer, sheet_name='Summary', index=False, encoding='utf-8-sig', freeze_panes=(1, 0))
        try:
            df_sql = df_m.copy()
            df_sql.columns = convert_cols_db(df_sql.columns)
            df_sql.to_sql('comparison', self.conn, if_exists='replace', index=False)
        except Exception as e:
            logger.error(e, exc_info=sys.exc_info())
        return df_summary

    def close_connection(self):
        self.conn.close()


def main():
    logger.info("Comparing external data with data collected internally")
    dc = DataComparison()
    try:
        dc.concatenate_ticker_exchange()
        dc.file_for_rpds()
        return dc.compare()
    except Exception as e:
        logger.error(e, exc_info=sys.exc_info())
        error_email(str(e))
    finally:
        dc.close_connection()


if __name__ == '__main__':
    main()
