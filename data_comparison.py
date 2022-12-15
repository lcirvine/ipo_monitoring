import os
import sys
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
        self.ref_folder = os.path.join(os.getcwd(), 'Reference')
        self.conn = pg_connection()
        self.df_pp = self.pipe_data()
        self.df_e = self.entity_data()
        self.df_s = self.source_data()

    def pipe_data(self):
        date_cols = ['announcement_date', 'pricing_date', 'trading_date', 'closing_date', 'last_updated_date_utc']
        conn_tc = pg_connection(database='termcond')
        tc_query = """
        SELECT 
            md.iconum
            ,agnt.as_reported_name AS company_name
            ,md.id AS master_deal
            ,ms.cusip::TEXT AS cusip
            ,CONCAT(md.id::TEXT, '.NI') AS client_deal_id
            ,secl.ticker::TEXT
            ,ex.description as exchange
            ,secis.offering_price as price
            ,secis.min_offering_price
            ,secis.max_offering_price
            ,secdt.announcement_date
            ,secdt.pricing_date
            ,secdt.trading_date
            ,secdt.closing_date
            ,lps.description as deal_status
            ,dl.last_updated_date_utc
        FROM
            pipe.v_tc_pipe_deal dl
            INNER JOIN pipe.tc_pipe_master_deal md ON dl.master_deal = md.id
            INNER JOIN pipe.tc_pipe_deal_issuer dlisr ON dl.id = dlisr.deal
            INNER JOIN pipe.tc_pipe_deal_agent dlagnt ON dlisr.pipe_agent = dlagnt.id
            INNER JOIN dbo.tc_agents agnt ON dlagnt.agent = agnt.agent_id
            INNER JOIN pipe.tc_pipe_security sec ON dl.id = sec.deal
            INNER JOIN pipe.tc_pipe_security_details secd ON sec.id = secd.security AND secd.ipo_flag = 1
            INNER JOIN pipe.tc_pipe_master_security ms ON sec.master_security = ms.id
            INNER JOIN pipe.tc_pipe_security_dates secdt ON sec.id = secdt.security
            LEFT JOIN pipe.tc_pipe_security_listing secl ON sec.id = secl.security
            INNER JOIN pipe.tc_pipe_security_issuance_subscription secis ON sec.id = secis.security
            INNER JOIN pipe.tc_pipe_lookup_placement_status lps ON secd.placement_status = lps.id
            LEFT JOIN dbo.SecmasExchanges ex ON secl.exchange = ex.exchange_code
        WHERE
            dl.rn = 1
            AND dl.last_updated_date_utc > (CURRENT_DATE - INTERVAL '7 day')"""
        df_new = pd.read_sql_query(tc_query, conn_tc, parse_dates=date_cols)
        conn_tc.close()
        df_existing = pd.read_sql_table('peo_pipe', self.conn, coerce_float=False, parse_dates=date_cols)
        df = pd.concat((df_existing, df_new), ignore_index=True, sort=False)
        df['exchange'] = df['exchange'].str.strip()
        df['deal_status'] = df['deal_status'].str.strip()
        df.sort_values(by='last_updated_date_utc', ascending=False, inplace=True)
        # numeric tickers could appear as duplicates if the same ticker has been interpreted as numeric and string
        # Also could have duplicates if ticker is initially NA, then later added for the same master deal
        df.drop_duplicates(subset=['iconum', 'master_deal', 'ticker'], inplace=True)
        try:
            df.to_sql('peo_pipe', self.conn, if_exists='replace', index=False)
        except Exception as e:
            logger.error(e, exc_info=sys.exc_info())
        return df

    def entity_data(self):
        return pd.read_sql_table('entity_mapping', self.conn)

    def source_data(self):
        return pd.read_sql_table('all_ipos', self.conn)

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

        df_cusip = self.df_pp[['iconum', 'cusip']].dropna(subset=['cusip'])
        df_cusip.drop_duplicates(inplace=True)
        df_cusip['cusip'] = df_cusip['cusip'].astype(str)
        df_cusip = df_cusip.groupby('iconum')['cusip'].apply(', '.join).reset_index()
        self.df_pp = self.df_pp.merge(df_cusip, how='left', on='iconum', suffixes=('_drop', ''))

        self.df_pp.drop(columns=['ticker_drop', 'exchange_drop', 'cusip_drop'], inplace=True)
        self.df_pp = self.df_pp[['iconum', 'cusip', 'company_name', 'master_deal', 'cusip', 'client_deal_id', 'ticker',
                                 'exchange', 'price', 'min_offering_price', 'max_offering_price', 'announcement_date',
                                 'pricing_date', 'trading_date', 'closing_date', 'deal_status',
                                 'last_updated_date_utc']]
        self.df_pp.sort_values('last_updated_date_utc', ascending=False, inplace=True)
        self.df_pp.drop_duplicates(subset=['iconum', 'master_deal'], inplace=True)

    def merge_entity_data(self):
        # merge data from sources with entities data
        df_m = pd.merge(self.df_s, self.df_e[['company_name', 'entityname', 'iconum']], how='left')
        # Chinese exchanges will have company name in Chinese
        # the Concordance API will rarely return an iconum for a Chinese name
        # If there is no iconum for a company on a Chinese exchange, try to compare PEO-PIPE data based on the Symbol
        cn_exch = ['Shenzhen Stock Exchange', 'Shanghai Stock Exchange', 'Shanghai', 'Shenzhen']
        pp_cn = self.df_pp.loc[(self.df_pp['exchange'].isin(cn_exch)) & (~self.df_pp['ticker'].isna())]
        pp_cn = pp_cn[['iconum', 'company_name', 'ticker']]
        df_m = pd.merge(df_m, pp_cn, how='left', on='ticker', suffixes=('', '_cn'))
        df_m['iconum'].fillna(df_m['iconum_cn'], inplace=True)
        df_m['entityname'].fillna(df_m['company_name_cn'], inplace=True)
        # for Chinese Exchanges, get the English name from concordance API or PEO-PIPE data if possible
        df_m.loc[(df_m['exchange'].isin(cn_exch)) & ~df_m['entityname'].isna(), 'company_name'] = df_m['entityname']
        df_m.drop(columns=['iconum_cn', 'company_name_cn', 'ticker'], inplace=True)
        return df_m

    def file_for_rpds(self):
        """
        Creating a file that will be used to create RPDs for Symbology and Fundamentals teams.
        This will have all IPOs both from external sources and collected internally.
        :return:
        """
        pp_cols = ['iconum', 'cusip', 'company_name', 'client_deal_id', 'ticker', 'exchange', 'price', 'trading_date',
                   'last_updated_date_utc']
        df_outer = pd.merge(self.merge_entity_data(), self.df_pp[pp_cols], how='outer', on='iconum',
                            suffixes=('_external', '_fds'))
        for col in ['ipo_date', 'trading_date']:
            df_outer[col] = pd.to_datetime(df_outer[col], errors='coerce')
        # TODO: is this error still happening?
        # sometimes I get this error Reindexing only valid with uniquely valued Index objects
        # probably due to this bug https://github.com/pandas-dev/pandas/issues/39882
        # should be fixed in v 1.3, but leaving comments for reference https://github.com/pandas-dev/pandas/milestone/80
        df_ipo = df_outer.loc[
            (df_outer['ipo_date'].dt.date >= date.today())
            | (df_outer['trading_date'].dt.date >= date.today())
        ]
        # TODO: save this data to the database
        df_ipo.to_excel(os.path.join(self.ref_folder, 'IPO Monitoring Data.xlsx'), index=False)
        df_wd = df_outer.loc[df_outer['status'] == 'Withdrawn']
        df_wd.to_excel(os.path.join(self.ref_folder, 'Withdrawn IPOs.xlsx'), index=False)

    def compare(self):
        # no longer needed, leaving method but not calling it
        df_m = pd.merge(self.merge_entity_data(), self.df_pp, how='left', on='iconum', suffixes=('_external', '_fds'))
        df_m.drop_duplicates(inplace=True)
        for c in [col for col in df_m.columns if 'date' in col.lower()]:
            # intermittently getting InvalidIndexError: Reindexing only valid with uniquely valued Index objects
            try:
                df_m[c] = pd.to_datetime(df_m[c].fillna(pd.NaT), errors='coerce').dt.date
            except Exception as e:
                logger.error(f"{c} - {e}")
        df_m['ipo_dates_match'] = df_m['ipo_date'] == df_m['trading_date']
        df_m['ipo_prices_match'] = df_m['price_external'] == df_m['price_fds']
        df_m.loc[df_m['price_external'].isna(), 'ipo_prices_match'] = True
        df_m.loc[df_m['ipo_date'].isna(), 'ipo_dates_match'] = True
        df_m = df_m[['ipo_dates_match', 'ipo_prices_match', 'iconum', 'company_name_external', 'exchange_external',
                     'ipo_date', 'price_external', 'price_range', 'status', 'notes', 'time_added', 'company_name_fds',
                     'master_deal', 'client_deal_id', 'cusip', 'ticker', 'exchange_fds', 'price_fds',
                     'min_offering_price', 'max_offering_price', 'announcement_date', 'pricing_date', 'trading_date',
                     'closing_date', 'deal_status', 'last_updated_date_utc']]
        df_m.drop_duplicates(inplace=True)
        df_m.to_sql('comparison', self.conn, if_exists='replace', index=False)
        return df_m

    def close_connection(self):
        self.conn.close()


def main():
    logger.info("Comparing external data with data collected internally")
    dc = DataComparison()
    try:
        dc.concatenate_ticker_exchange()
        dc.file_for_rpds()
    except Exception as e:
        logger.error(e, exc_info=sys.exc_info())
        error_email(str(e))
    finally:
        dc.close_connection()


if __name__ == '__main__':
    main()
