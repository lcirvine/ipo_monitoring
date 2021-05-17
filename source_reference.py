import json
import pandas as pd
import configparser

config = configparser.ConfigParser()
config.read('api_key.ini')

website_sources = {
    'NYSE': {
        'exchange': 'NYSE',
        'rank': 1,
        'location': 'New York',
        'url': 'https://www.nyse.com/ipo-center/filings',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'class': 'table-data'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'ipo_date',
            'company_name',
            'ticker',
            'industry',
            'underwriters',
            'exchange',
            'deal_size',
            'shares_offered',
            'price_range'
        ],
        'column_names_as_row': False,
        'file': 'NYSE',
        'db_table_raw': 'source_nyse_raw',
        'db_table': 'source_nyse'
    },
    'NYSE Withdrawn': {
        'exchange': 'NYSE',
        'rank': 1,
        'location': 'New York',
        'url': 'https://www.nyse.com/ipo-center/filings',
        'table_num': 3,
        'table_elem': 'table',
        # 'table_attrs': {'class': 'table-data'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'postponement_date',
            'company_name',
            'ticker',
            'industry',
            'underwriters',
            'deal_size',
            'shares_offered',
            'status'
        ],
        'column_names_as_row': False,
        'file': 'NYSE Withdrawn',
        'db_table_raw': 'source_nyse_withdrawn_raw',
        'db_table': 'source_nyse_withdrawn'
    },
    'Nasdaq': {
        'exchange': 'NASDAQ',
        'rank': 2,
        'location': 'New York',
        'url': 'https://www.nasdaq.com/market-activity/ipos?tab=upcoming',
        'table_num': 2,
        'table_elem': 'tbody',
        # 'table_attrs': {'class': 'market-calendar-table__body'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': ['th', 'td'],
        # 'cell_attrs': {},
        'header_elem': 'th',
        'header_attrs': {'class': ['market-calendar-table__columnheader']},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'ticker',
            'company_name',
            'exchange',
            'price',
            'shares_offered',
            'ipo_date',
            'deal_size'
        ],
        'column_names_as_row': False,
        'file': 'Nasdaq',
        'db_table_raw': 'source_nasdaq_raw',
        'db_table': 'source_nasdaq'
    },
    'Nasdaq Priced': {
        'exchange': 'NASDAQ',
        'rank': 2,
        'location': 'New York',
        'url': 'https://www.nasdaq.com/market-activity/ipos?tab=upcoming',
        'table_num': 3,
        'table_elem': 'tbody',
        # 'table_attrs': {'class': 'market-calendar-table__body'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': ['th', 'td'],
        # 'cell_attrs': {},
        'header_elem': 'th',
        'header_attrs': {'class': ['market-calendar-table__columnheader']},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'ticker',
            'company_name',
            'exchange',
            'price',
            'shares_offered',
            'ipo_date',
            'deal_size',
            'status'
        ],
        'column_names_as_row': False,
        'file': 'Nasdaq Priced',
        'db_table_raw': 'source_nasdaq_priced_raw',
        'db_table': 'source_nasdaq_priced'
    },
    'Nasdaq Withdrawn': {
        'exchange': 'NASDAQ',
        'rank': 2,
        'location': 'New York',
        'url': 'https://www.nasdaq.com/market-activity/ipos?tab=upcoming',
        'table_num': 5,
        'table_elem': 'tbody',
        # 'table_attrs': {'class': 'market-calendar-table__body'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': ['th', 'td'],
        # 'cell_attrs': {},
        'header_elem': 'th',
        'header_attrs': {'class': ['market-calendar-table__columnheader']},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'ticker',
            'company_name',
            'exchange',
            'shares',
            'announcement_date',
            'deal_size',
            'cancellation_date'
        ],
        'column_names_as_row': False,
        'file': 'Nasdaq Withdrawn',
        'db_table_raw': 'source_nasdaq_withdrawn_raw',
        'db_table': 'source_nasdaq_withdrawn'
    },
    'JPX': {
        'exchange': 'Japan Exchange Group',
        'rank': 3,
        'location': 'Tokyo',
        'url': 'https://www.jpx.co.jp/english/listing/stocks/new/',
        'table_num': 0,
        'table_elem': 'table',
        # 'table_attrs': {},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'ipo_date',
            'date_of_listing_approval',
            'company_name',
            'ticker',
            'market_segment',
            'document_link'

        ],
        'column_names_as_row': False,
        'file': 'JPX',
        'db_table_raw': 'source_jpx_raw',
        'db_table': 'source_jpx'
    },
    'Shanghai': {
        'exchange': 'Shanghai Stock Exchange',
        'rank': 4,
        'location': 'Shanghai',
        'url': 'http://ipo.sseinfo.com/info/xgfxyl/',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'id': 'ipoDataList_container'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'new_share_name',
            'subscription_date',
            'price',
            'initial_shares_total_shares',
            'deal_size',
            'pe_ratio',
            'online_and_offline_circulation',
            'online_purchase_limit',
            'success_rate',
            'announcement_of_winning_results',
            'listing_date'
        ],
        'column_names_as_row': False,
        'file': 'Shanghai',
        'db_table_raw': 'source_shanghai_raw',
        'db_table': 'source_shanghai'
    },
    'Euronext': {
        'exchange': 'Euronext',
        'rank': 5,
        'location': ', '.join(['Amsterdam', 'Brussels', 'Dublin', 'Lisbon', 'London', 'Oslo', 'Paris']),
        # ['Amsterdam', 'Brussels', 'Dublin', 'Lisbon', 'London', 'Oslo', 'Paris'],
        'url': 'https://live.euronext.com/en/ipo-showcase',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'class': ['table', 'views-table', 'views-view-table', 'cols-5']},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'ipo_date',
            'company_name',
            'isin',
            'location',
            'exchange'
        ],
        'column_names_as_row': False,
        'file': 'Euronext',
        'db_table_raw': 'source_euronext_raw',
        'db_table': 'source_euronext'
    },
    'AAStocks': {
        'exchange': 'Hong Kong Exchange',
        'rank': 6,
        'location': 'Hong Kong',
        'url': 'https://www.aastocks.com/en/stocks/market/ipo/upcomingipo/company-summary',
        'table_num': 24,
        'table_elem': 'table',
        'table_attrs': {'id': 'tblUpcoming'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'blank',
            'ticker',
            'company_name',
            'industry',
            'price',
            'lot_size',
            'entry_fee',
            'subscription_period',
            'ipo_date'
        ],
        'column_names_as_row': True,
        'file': 'AAStocks',
        'db_table_raw': 'source_aastocks_raw',
        'db_table': 'source_aastocks'
    },
    'LSE': {
        'exchange': 'London Stock Exchange',
        'rank': 7,
        'location': 'London',
        'url': 'https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/new-and-recent-issues/new-recent-issues-home.html',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'class': 'upcoming-issues'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'company_name',
            'exchange',
            'deal_size',
            'deal_size_secondary',
            'currency',
            'price_range',
            'ipo_date',
            'security_type'
        ],
        'column_names_as_row': False,
        'file': 'LSE',
        'db_table_raw': 'source_lse_raw',
        'db_table': 'source_lse'
    },
    'CNInfo': {
        'exchange': 'Shenzhen Stock Exchange',
        'rank': 8,
        'location': 'Shenzhen',
        'url': 'http://www.cninfo.com.cn/eipo/index.jsp?COLLCC=1039335625&',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'id': 'newstock_table'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'ticker',
            'company_name',
            'release_date',
            'price',
            'pe_ratio',
            'total_shares_offered',
            'online_shares_offered',
            'shares_offered',
            'existing_shares_offered',
            'subscription_limit',
            'success_rate',
            'announcement_of_winning_results',
            'ipo_date'
        ],
        'column_names_as_row': False,
        'file': 'CNInfo',
        'db_table_raw': 'source_cninfo_raw',
        'db_table': 'source_cninfo'
    },
    'BS-TSX': {
        'exchange': 'TSX',
        'rank': None,
        'location': 'Toronto',
        'url': 'https://www.baystreet.ca/articles/ipo_tsx.aspx',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'class': 'gridview'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        # 'header_elem': None,
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'company_name',
            'ticker',
            'ipo_date'
        ],
        'column_names_as_row': False,
        'file': 'BS-TSX',
        'db_table_raw': 'source_bs_tsx_raw',
        'db_table': 'source_bs_tsx'
    },
    'BS-TSXV': {
        'exchange': 'TSX',
        'rank': None,
        'location': 'Toronto',
        'url': 'https://www.baystreet.ca/articles/ipo_tsxv.aspx',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'class': 'gridview'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        # 'header_elem': None,
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'company_name',
            'ticker',
            'ipo_date'
        ],
        'column_names_as_row': False,
        'file': 'BS-TSXV',
        'db_table_raw': 'source_bs_tsxv_raw',
        'db_table': 'source_bs_tsxv'
    },
    'TSX': {
        'exchange': 'Toronto Stock Exchange',
        'rank': 9,
        'location': 'Toronto',
        'url': 'https://www.tsx.com/news/new-company-listings',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'class': 'two-columns-list'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'ipo_date',
            'company_name'
        ],
        'column_names_as_row': False,
        'file': 'TSX',
        'db_table_raw': 'source_tsx_raw',
        'db_table': 'source_tsx'
    },
    'Frankfurt': {
        'exchange': 'Deutsche Boerse',
        'rank': 12,
        'location': 'Frankfurt',
        'url': 'https://www.deutsche-boerse-cash-market.com/dbcm-en/instruments-statistics/statistics/primary-market-statistics/432!search?&hitsPerPage=50',
        'table_num': 0,
        'table_elem': 'ol',
        'table_attrs': {'class': ['list', 'search-results']},
        'row_elem': 'li',
        # 'row_attrs': {},
        'cell_elem': 'div',
        # 'cell_attrs': {},
        # 'header_elem': None,
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'ipo_date',
            'summary',
            'market_segment',
            'sector',
            'sub_price_and_deal_size',
            'first_price_and_market_cap'
        ],
        'column_names_as_row': False,
        'file': 'Frankfurt',
        'db_table_raw': 'source_frankfurt_raw',
        'db_table': 'source_frankfurt'
    },
    'KRX': {
        'exchange': 'Korea Exchange',
        'rank': 13,
        'location': 'Seoul',
        'url': 'https://global.krx.co.kr/contents/GLB/03/0306/0306010000/GLB0306010000.jsp',
        'table_num': 1,
        'table_elem': 'table',
        'table_attrs': {'class': 'CI-GRID-BODY-TABLE'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'ticker',
            'company_name',
            'ipo_date',
            'shares_outstanding',
            'par_value',
            'price',
            'industry',
            'underwriters'
        ],
        'column_names_as_row': False,
        'file': 'KRX',
        'db_table_raw': 'source_krx_raw',
        'db_table': 'source_krx'
    },
    'TWSE': {
        'exchange': 'Taiwan Stock Exchange',
        'rank': 18,
        'location': 'Taipei',
        'url': 'https://www.twse.com.tw/en/page/listed/listed_company/new_listing.html',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'id': 'report-table'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'ticker',
            'company_name',
            'announcement_date',
            'capital_thousands_twd',
            'listing_review_date',
            'application_approval_date',
            'listing_agreement_submitted_to_fsc_date',
            'ipo_date',
            'underwriters',
            'price',
            'note'
        ],
        'column_names_as_row': False,
        'file': 'TWSE',
        'db_table_raw': 'source_twse_raw',
        'db_table': 'source_twse'
    },
    'BME': {
        'exchange': 'BME Spanish Exchanges',
        'rank': 20,
        'location': 'Madrid',
        'url': 'https://www.bolsamadrid.es/ing/aspx/Empresas/Admisiones.aspx',
        'table_num': 3,
        'table_elem': 'table',
        'table_attrs': {'class': 'TblPort'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'ipo_date',
            'isin',
            'bolsa_code',
            'company_name',
            'shares_offered',
            'deal_size',
            'volume',
            'listing_type',
            'observations'
        ],
        'column_names_as_row': False,
        'file': 'BME',
        'db_table_raw': 'source_bme_raw',
        'db_table': 'source_bme'
    },
    'SGX': {
        'exchange': 'Singapore Exchange',
        'rank': 21,
        'location': 'Singapore',
        'url': 'https://www.sgx.com/securities/ipo-performance',
        'table_num': 0,
        'table_elem': 'sgx-table-list',
        # 'table_attrs': {'class': 'sgx-table-list'},
        'row_elem': 'sgx-table-row',
        # 'row_attrs': {},
        'cell_elem': ['sgx-table-cell-link', 'sgx-table-cell-text', 'sgx-table-cell-date', 'sgx-table-cell-number'],
        # 'cell_attrs': {},
        # 'header_elem': None,
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'company_name',
            'country_incorporation',
            'ipo_date',
            'underwriter',
            'price',
            'deal_size',
            'market_cap_ipo_mm',
            'closing_price_first_day',
            'change_from_ipo_price',
            'closing_price_prev_day',
            'market_cap_prev_day_mm',
            'prem_disc_to_ipo_price',
            'market_segment'
        ],
        'column_names_as_row': False,
        'file': 'SGX',
        'db_table_raw': 'source_sgx_raw',
        'db_table': 'source_sgx'
    },
    'IDX': {
        'exchange': 'Indonesia Stock Exchange',
        'rank': 24,
        'location': 'Jakarta',
        'url': 'https://www.idx.co.id/en-us/listed-companies/listing-activities/',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'id': 'ipoTable'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'num',
            'ticker',
            'company_name',
            'ipo_date',
            'delisting_date',
            'shares_offered',
            'market_segment'
        ],
        'column_names_as_row': False,
        'file': 'IDX',
        'db_table_raw': 'source_idx_raw',
        'db_table': 'source_idx'
    },
    'BM': {
        'exchange': 'Bursa Malaysia',
        'rank': 25,
        'location': 'Kuala Lumpur',
        'url': 'https://www.bursamalaysia.com/listing/listing_resources/ipo/ipo_summary',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'class': 'data-table'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'company_name',
            'subscription_date_start',
            'subscription_date_end',
            'price',
            'shares_offered_public',
            'shares_offer_for_sale',
            'shares_offered_private_placement',
            'issuing_house_ac_no',
            'market_segment',
            'ipo_date'
        ],
        'column_names_as_row': False,
        'file': 'BM',
        'db_table_raw': 'source_bm_raw',
        'db_table': 'source_bm'
    },
    'BIT': {
        'exchange': 'Borsa Italiana',
        'rank': None,
        'location': 'Milan',
        'url': 'https://www.borsaitaliana.it/azioni/ipoematricole/ipo-home.en.htm',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'class': ['m-table', '-editorial']},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'company_name',
            'listing_type',
            'ipo_date',
            'market_segment'
        ],
        'column_names_as_row': True,
        'file': 'BIT',
        'db_table_raw': 'source_bit_raw',
        'db_table': 'source_bit'
    },
    'IPOScoop': {
        'exchange': 'NYSE and Nasdaq',
        # 'rank': None
        'location': 'New York',
        'url': 'https://www.iposcoop.com/ipo-calendar/',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'class': 'ipolist'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'company_name',
            'ticker',
            'underwriters',
            'shares_offered_mm',
            'price_range_low',
            'price_range_high',
            'volume',
            'ipo_date',
            'scoop_rating',
            'rating_change'
        ],
        'column_names_as_row': False,
        'file': 'IPOScoop',
        'db_table_raw': 'source_iposcoop_raw',
        'db_table': 'source_iposcoop'
    },
    'NasdaqNordic': {
        'exchange': 'Multiple',
        'rank': None,
        'location': ', '.join(['Copenhagen', 'Helsinki', 'Iceland', 'Stockholm']),
        # ['Copenhagen', 'Helsinki', 'Iceland', 'Stockholm']
        'url': 'http://www.nasdaqomxnordic.com/',
        'table_num': 2,
        'table_elem': 'table',
        'table_attrs': {'id': 'latestListingsTable'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        # 'header_elem': None,
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'company_name',
            'last_price',
            'percent_change',
            'ipo_date'
        ],
        'column_names_as_row': False,
        'file': 'NasdaqNordic',
        'db_table_raw': 'source_nasdaqnordic_raw',
        'db_table': 'source_nasdaqnordic'
    },
    'Spotlight': {
        'exchange': 'Spotlight',
        'rank': None,
        'location': 'Stockholm',
        'url': 'https://spotlightstockmarket.com/en/market-overview/listings/',
        'table_num': 1,
        'table_elem': 'table',
        'table_attrs': None,
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        # 'header_elem': None,
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'subscription_period',
            'ipo_date',
            'company_name',
            'listing_type',
            'document_link'
        ],
        'column_names_as_row': False,
        'file': 'Spotlight',
        'db_table_raw': 'source_spotlight_raw',
        'db_table': 'source_spotlight'
    },
    'East Money': {
        'exchange': ', '.join(['Shanghai Stock Exchange', 'Shenzhen Stock Exchange']),
        'rank': 6,
        'location': ', '.join(['Shanghai', 'Shenzhen']),
        'url': 'http://data.eastmoney.com/xg/xg/default.html',
        'table_num': 1,
        'table_elem': 'table',
        # 'table_attrs': {'attr_key': 'attr_value'},
        'row_elem': 'tr',
        # 'row_attrs': {'attr_key': 'attr_value'},
        'cell_elem': 'td',
        # 'cell_attrs': {'attr_key': 'attr_value'},
        # 'header_elem': 'th',
        # 'header_attrs': {'attr_key': 'attr_value'},
        # 'link_elem': 'a',
        'columns': [
            'ticker',
            'company_name',
            'relevant_information',
            'subscription_code',
            'shares_offered_total',
            'shares_offered_online',
            'top_grid_purchase_match_mkt_value',
            'subscription_limit',
            'price',
            'latest_price',
            'closing_price_first_day',
            'subscription_date',
            'announcement_of_winning_results',
            'funding_date',
            'ipo_date',
            'pe_ratio',
            'pe_ratio_industry',
            'success_rate',
            'number_of_inquiries',
            'number_of_allotments',
            'market_segment',
            'percent_change',
            'change_from_ipo_price',
            'document_link'
        ],
        'column_names_as_row': False,
        'file': 'East Money',
        'db_table_raw': 'source_east_money_raw',
        'db_table': 'source_east_money'
    }
}

api_sources = {
    'AlphaVantage': {
        'exchange': 'NYSE and Nasdaq',
        'location': 'New York',
        'endpoint': 'https://www.alphavantage.co/query',
        'parameters': {
            'function': 'IPO_CALENDAR',
            'apikey': config.get('AV', 'key')
        },
        'rename_columns': {
            'symbol': 'ticker',
            'name': 'company_name',
            'ipoDate': 'ipo_date',
            'priceRangeLow': 'price_range_low',
            'priceRangeHigh': 'price_range_high'
        },
        'file': 'AlphaVantage-US',
        'db_table_raw': 'source_alphavantage_raw',
        'db_table': 'source_alphavantage'
    },
    'SpotlightAPI': {
        'exchange': 'Spotlight',
        'location': 'Stockholm',
        'endpoint': 'http://api.spotlightstockmarket.com/v1/listing',
        'rename_columns': {
            'Id': 'num',
            'DateFrom': 'subscription_date_start',
            'DateTo': 'subscription_date_end',
            'ListingDate': 'ipo_date',
            'CompanyName': 'company_name',
            'EmissionDescriptionEnglish': 'listing_type'
        },
        'file': 'SpotlightAPI',
        'db_table_raw': 'source_spotlight_raw',
        'db_table': 'source_spotlight'
    }
}

rejected_sources = {
    'ASX': {
        'exchange': 'Australian Stock Exchange',
        'rank': 16,
        'location': 'Sydney',
        # 'url': 'https://www2.asx.com.au/listings/upcoming-floats-and-listings',
        'table_num': 0,
        'table_elem': 'div',
        'table_attrs': {'class': 'accordion'},
        'row_elem': 'div',
        'row_attrs': {'class': 'accordion__item__header'},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        # 'header_elem': None,
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'Company Name',
            'Listing date',
            'Company contact details',
            'Principal Activities',
            'GICS industry group',
            'Issue Price',
            'Issue Type',
            'Security code',
            'Capital to be Raised',
            'Expected offer close date',
            'Underwriter'
        ],
        'file': 'ASX'
    },
    'IPOHub': {
        'exchange': 'Multiple',
        # 'rank': None,
        # 'location': None,
        # 'url': 'https://www.ipohub.io/listings?current-tab=upcoming&view=list&type=ipo&type=listing&take=80',
        # 'table_num': None,
        'table_elem': 'div',
        'table_attrs': {'class': 'latest-offer-table'},
        'row_elem': 'a',
        # 'row_attrs': {},
        'cell_elem': 'div',
        # 'cell_attrs': {},
        # 'header_elem': None,
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'Company',
            'Sector',
            'Type',
            'Subscription period',
            'First Trading Date',
            'Price',
            'Pre-money Valuation',
            'Follow'
        ],
        'file': 'IPOHub'
    },
    'TokyoIPO': {
        'exchange': 'Multiple',
        'rank': 3,
        'location': 'Tokyo',
        # 'url': 'http://www.tokyoipo.com/top/iposche/index.php?j_e=E',
        'table_num': 3,
        'table_elem': 'table',
        'table_attrs': {'class': 'iposchedulelist'},
        'row_elem': 'a',
        # 'row_attrs': {},
        'cell_elem': 'div',
        # 'cell_attrs': {},
        # 'header_elem': None,
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'Company',
            'Sector',
            'Type',
            'Subscription period',
            'First Trading Date',
            'Price',
            'Pre-money Valuation',
            'Follow'
        ],
        'file': 'TokyoIPO'
    },
    'Nyemissioner': {
        'exchange': 'Multiple',
        'rank': None,
        'location': 'Sweden',
        # 'url': 'https://nyemissioner.se/foretag/planerad-noteringar/sokning/95861',
        'table_num': 2,
        'table_elem': 'table',
        'table_attrs': {'class': ['rows', 'row-links']},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        # 'header_elem': None,
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'Company Name',
            'Industry',
            'List',
            'Subscription period'
        ],
        'file': 'Nyemissioner'
    },
    'Chittorgarh': {
        'exchange': 'NSE and BSE',
        # 'rank': None,
        'location': 'Mumbai',
        # 'url': 'https://www.chittorgarh.com/ipo/ipo_list.asp',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'class': 'table'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'Issuer Company',
            'Exchange',
            'Open',
            'Close',
            'Lot Size',
            'Issue Price (Rs)',
            'Issue Size (Rs Cr)'
        ],
        'file': 'Chittorgarh'
    },
    'NSE': {
        'exchange': 'National Stock Exchange of India',
        'rank': 10,
        'location': 'Mumbai',
        'url': 'https://www.nseindia.com/market-data/all-upcoming-issues-ipo',
        'table_num': 0,
        'table_elem': 'table',
        'table_attrs': {'id': 'publicIssuesCurrent'},
        'row_elem': 'tr',
        # 'row_attrs': {},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'Company Name',
            'Security type',
            'Issue Start Date',
            'Issue End Date',
            'Status',
            'offered/ reserved',
            'Bids',
            'Subscription Category'
        ],
        'file': 'NSE'
    },
    'BSE': {
        'exchange': 'Bombay Stock Exchange',
        'rank': 11,
        'location': 'Mumbai',
        'url': 'https://www.bseindia.com/publicissue.html',
        'table_num': 3,
        'table_elem': 'table',
        'table_attrs': {'ng-init': 'fn_Livepubilicissue()'},
        'row_elem': 'tr',
        'row_attrs': {'class': 'ng-scope'},
        'cell_elem': 'td',
        # 'cell_attrs': {},
        'header_elem': 'th',
        # 'header_attrs': {},
        # 'link_elem': '',
        # 'link_key': '',
        'columns': [
            'Security Name',
            'Start Date',
            'End Date',
            'Offer Price',
            'Face Value',
            'Type Of Issue',
            'Issue Status'
        ],
        'file': 'BSE'
    }
}


def create_json_file(file_name: str = 'sources'):
    with open(file_name + '.json', 'w') as f:
        json.dump(website_sources, f)


def return_sources(source_type: str = 'all') -> dict:
    """
    Returns a dictionary sources specified by the source_type.

    :param source_type: Valid source_types are 'all', 'website' and 'api'. Default source_type is 'all'.
    :return: dictionary of sources
    """
    valid_types = {'all', 'website', 'api'}
    if source_type not in valid_types:
        raise ValueError(f"{source_type} is not a valid source type. Please enter all, website or api.")
    if source_type == 'website':
        return website_sources
    elif source_type == 'api':
        return api_sources
    elif source_type == 'all':
        return dict(**website_sources, **api_sources)


def create_source_ref_file(file_name: str = 'sources'):
    ex_dict = return_sources(source_type='website')
    df = pd.DataFrame(ex_dict).transpose()
    if file_name != '':
        df.to_excel(file_name + '.xlsx', index_label='source', freeze_panes=(1, 0))
        df.to_csv(file_name + '.csv', index_label='source')


def main():
    create_json_file()
    create_source_ref_file()


if __name__ == '__main__':
    main()
