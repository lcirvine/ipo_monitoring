import os
import sys
import pyodbc
import pandas as pd
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
        df.drop_duplicates(subset=['iconum', 'master_deal', 'ticker'], inplace=True)
        df.to_excel(pp_file, index=False, encoding='utf-8-sig', freeze_panes=(1, 0))
        return df

    def entity_data(self):
        return pd.read_excel(os.path.join(self.ref_folder, 'Entity Mapping.xlsx'))

    def source_data(self):
        return pd.read_excel(os.path.join(self.results_folder, 'All IPOs.xlsx'))

    def update_entity_mapping(self):
        df_e_un = pd.merge(self.df_s, self.df_e, how='outer', on='Company Name', suffixes=('', '_ent'), indicator=True)
        df_e_un = df_e_un.loc[df_e_un['_merge'] == 'left_only']
        df_cn = df_e_un.loc[df_e_un['Market'].isin(['Shenzhen Stock Exchange', 'Shanghai Stock Exchange'])]
        df_e_un.drop(df_cn.index, inplace=True)
        df_e_un.dropna(subset=['Company Name'], inplace=True)
        if len(df_e_un['Company Name']) > 0:
            new_names = ', '.join("('" + df_e_un['Company Name'] + "')")
            query = f"""
                    SELECT 
                    [Name]
                    ,CASE 
                    WHEN dbo.ufn_entity_match([Name]) = 'No Match' THEN NULL
                    ELSE dbo.ufn_entity_match([Name])
                    END AS [entity_id]
                    ,CASE 
                    WHEN dbo.ufn_entity_match([Name]) = 'No Match' THEN NULL
                    ELSE dbo.ufn_fdsuid_to_entityid(dbo.ufn_entity_match([Name])) 
                    END AS iconum
                    FROM (VALUES
                    {new_names}
                    ) AS co_names([Name])
                    """
            df_e_new = pd.read_sql_query(query, self.return_db_connection('lion'))
            df_e_new = pd.merge(df_e_new, self.df_s[['Company Name', 'Symbol', 'Market']], how='left', left_on='Name', right_on='Company Name')
            self.df_e = pd.concat([self.df_e, df_e_new])
        df_cn = pd.merge(df_cn[['Company Name', 'Symbol', 'Market']], self.df_pp[['iconum', 'ticker']], how='left',
                         left_on='Symbol', right_on='ticker')
        df_cn.dropna(subset=['iconum'], inplace=True)
        if len(df_cn) > 0:
            self.df_e = pd.concat([self.df_e, df_cn])
        self.df_e.to_excel(os.path.join(self.ref_folder, 'Entity Mapping.xlsx'))

    def concatenate_ticker_exchange(self):
        self.df_pp.drop_duplicates(inplace=True)
        df_tickers = self.df_pp[['iconum', 'ticker']].dropna(subset=['ticker'])
        df_tickers['ticker'] = df_tickers['ticker'].astype(str)
        df_tickers.drop_duplicates(inplace=True)
        df_tickers = df_tickers.groupby('iconum')['ticker'].apply(', '.join).reset_index()
        df_exch = self.df_pp[['iconum', 'exchange']].dropna(subset=['exchange'])
        df_exch['exchange'] = df_exch['exchange'].str.strip()
        df_exch.drop_duplicates(inplace=True)
        df_exch = df_exch.groupby('iconum')['exchange'].apply(', '.join).reset_index()
        self.df_pp = self.df_pp.merge(df_tickers, how='left', on='iconum', suffixes=('_drop', ''))
        self.df_pp = self.df_pp.merge(df_exch, how='left', on='iconum', suffixes=('_drop', ''))
        self.df_pp.drop(columns=['ticker_drop', 'exchange_drop'], inplace=True)
        self.df_pp.drop_duplicates(inplace=True)
        self.df_pp = self.df_pp[['iconum', 'Company Name', 'master_deal', 'client_deal_id', 'ticker', 'exchange',
                                 'Price', 'min_offering_price', 'max_offering_price', 'announcement_date',
                                 'pricing_date', 'trading_date', 'closing_date', 'deal_status',
                                 'last_updated_date_utc']]

    def compare(self):
        df_m = pd.merge(self.df_s, self.df_e[['Company Name', 'iconum']], how='left')
        df_m = pd.concat([df_m, self.df_pp], axis=1, join='left')
        df_m = pd.merge(df_m, self.df_pp, how='left', on='iconum', suffixes=('_external', '_fds'))
        with pd.ExcelWriter(self.results_folder, 'IPO Monitoring.xlsx') as writer:
            df_m.to_excel(writer, sheet_name='Comparison', index=False, encoding='utf-8-sig', freeze_panes=(1, 0))
            self.df_s.to_excel(writer, sheet_name='Upcoming IPOs - External', index=False, encoding='utf-8-sig', freeze_panes=(1, 0))
            self.df_pp.to_excel(writer, sheet_name='PEO-PIPE IPO Data', index=False, encoding='utf-8-sig', freeze_panes=(1, 0))


def main():
    dc = DataComparison()
    try:
        dc.update_entity_mapping()
        dc.concatenate_ticker_exchange()
        dc.compare()
    except Exception as e:
        print(e)
        logger.error(e, exc_info=sys.exc_info())
        logger.info('-' * 100)


if __name__ == '__main__':
    main()
