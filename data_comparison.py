import os
import sys
import pyodbc
import pandas as pd
from datetime import date
from configparser import ConfigParser
from logging_ipo_dates import logger

pd.options.mode.chained_assignment = None


class DataComparison:
    def __init__(self):
        self.config = ConfigParser()
        self.config.read('db_connection.ini')
        self.results_folder = os.path.join(os.getcwd(), 'Results')
        self.ref_folder = os.path.join(os.getcwd(), 'Reference')
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
        if os.path.exists(pp_file):
            df = pd.concat([pd.read_excel(pp_file), df], ignore_index=True, sort=False)
        df['exchange'] = df['exchange'].str.strip()
        df.sort_values(by='last_updated_date_utc', ascending=False, inplace=True)
        # numeric tickers could appear as duplicates if the same ticker has been interpreted as numeric and string
        # Also could have duplicates if ticker is initially NA, then later added for the same master deal
        df['ticker'] = df['ticker'].astype(str)
        df.drop_duplicates(subset=['iconum', 'master_deal', 'ticker'], inplace=True)
        df.to_excel(pp_file, index=False, encoding='utf-8-sig', freeze_panes=(1, 0))
        return df

    def entity_data(self):
        return pd.read_excel(os.path.join(self.ref_folder, 'Entity Mapping.xlsx'))

    def source_data(self):
        return pd.read_excel(os.path.join(self.results_folder, 'All IPOs.xlsx'))

    def concatenate_ticker_exchange(self):
        self.df_pp.drop_duplicates(inplace=True)
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
        self.df_pp.drop(columns=['ticker_drop', 'exchange_drop'], inplace=True)
        self.df_pp.drop_duplicates(inplace=True)
        self.df_pp = self.df_pp[['iconum', 'Company Name', 'master_deal', 'client_deal_id', 'ticker', 'exchange',
                                 'Price', 'min_offering_price', 'max_offering_price', 'announcement_date',
                                 'pricing_date', 'trading_date', 'closing_date', 'deal_status',
                                 'last_updated_date_utc']]

    def compare(self):
        df_m = pd.merge(self.df_s, self.df_e[['Company Name', 'entityName', 'iconum']], how='left')
        # for Chinese Exchanges, get the English name if possible
        cn_exch = ['Shenzhen Stock Exchange', 'Shanghai Stock Exchange']
        df_m.loc[(df_m['Market'].isin(cn_exch)) & ~df_m['entityName'].isna(), 'Company Name'] = df_m['entityName']
        df_m = pd.merge(df_m, self.df_pp, how='left', on='iconum', suffixes=('_external', '_fds'))
        df_m.drop_duplicates(inplace=True)
        for c in [col for col in df_m.columns if 'date' in col.lower()]:
            df_m[c] = pd.to_datetime(df_m[c], errors='coerce').dt.date
        df_m['IPO Dates Match'] = df_m['IPO Date'] == df_m['trading_date']
        df_m['IPO Prices Match'] = df_m['Price_external'] == df_m['Price_fds']
        df_m.loc[df_m['Price_external'].isna(), 'IPO Prices Match'] = True
        df_m = df_m[['IPO Dates Match', 'IPO Prices Match', 'iconum', 'Company Name_external', 'Symbol', 'Market',
                     'IPO Date', 'Price_external', 'Price Range', 'Status', 'Notes', 'time_checked', 'Company Name_fds',
                     'master_deal', 'client_deal_id', 'ticker', 'exchange', 'Price_fds', 'min_offering_price',
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
        return df_summary


def main():
    dc = DataComparison()
    try:
        dc.concatenate_ticker_exchange()
        return dc.compare()
    except Exception as e:
        print(e)
        logger.error(e, exc_info=sys.exc_info())
        logger.info('-' * 100)


if __name__ == '__main__':
    main()
