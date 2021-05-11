import json
import pandas as pd

format_example = {
        'source': {
            'exchange': '',
            'rank': 0,
            'location': '',
            'url': '',
            'table_num': 0,
            'table_elem': 'table',
            'table_attrs': {'attr_key': 'attr_value'},
            'row_elem': 'tr',
            'row_attrs': {'attr_key': 'attr_value'},
            'cell_elem': 'td',
            'cell_attrs': {'attr_key': 'attr_value'},
            'header_elem': 'th',
            'header_attrs': {'attr_key': 'attr_value'},
            'link_elem': 'a',
            'link_key': 'href',
            'columns': [

            ],
            'file': ''
        },
}

sources_dict = {
    'NYSE': {
        'exchange': 'NYSE',
        'rank': 1,
        'location': 'New York',
        'url': 'https://www.nyse.com/ipo-center/filings',
        'text_marker': 'Expected Deals',
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
            'Expected Date',
            'Issuer',
            'Ticker',
            'Industry',
            'Bookrunner(S)',
            'Exchange',
            'Curr. Amt. Filed ($MM)',
            'Curr. Shrs. Filed ($MM)',
            'Curr. File Price/Range($)'
        ],
        'file': 'NYSE'
    },
    'NYSE Withdrawn': {
        'exchange': 'NYSE',
        'rank': 1,
        'location': 'New York',
        'url': 'https://www.nyse.com/ipo-center/filings',
        'text_marker': 'Withdrawn Deals',
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
            'Date W/P',
            'Issuer',
            'Ticker',
            'Industry',
            'Bookrunner(S)',
            'Amt. Filed ($MM)',
            'Shrs. Filed ($MM)',
            'Status'
        ],
        'file': 'NYSE Withdrawn'
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
            'Symbol',
            'Company Name',
            'Exchange/ Market',
            'Price',
            'Shares',
            'Expected IPO Date',
            'Offer Amount'
        ],
        'file': 'Nasdaq'
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
            'Symbol',
            'Company Name',
            'Exchange/ Market',
            'Price',
            'Shares',
            'Date',
            'Offer Amount',
            'Actions'
        ],
        'file': 'Nasdaq Priced'
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
            'Symbol',
            'Company Name',
            'Exchange/ Market',
            'Shares',
            'Date Filed',
            'Offer Amount',
            'Date Withdrawn'
        ],
        'file': 'Nasdaq Withdrawn'
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
            'Date of Listing',
            'Date of Listing Approval',
            'Issue Name',
            'Code',
            'Market Division',
            'Outline of Listing Issue'

        ],
        'file': 'JPX'
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
            'New Share Name',
            'Subscription Date',
            'Issue price',
            'Initial total issuance total issuance',
            'Actual funds raised',
            'Issue price-earnings ratio',
            'Online circulation offline circulation',
            'Online purchase limit',
            'Success rate (%)',
            'Announcement Day of Winning Results',
            'Listing date'
        ],
        'file': 'Shanghai'
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
            'Date',
            'Company name',
            'ISIN code',
            'Location',
            'Market'
        ],
        'file': 'Euronext'
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
            'Blank',
            'Code▼',
            'Name',
            'Industry',
            'Offer Price',
            'Lot Size',
            'Entry Fee',
            'Offer Period',
            'Listing Date'
        ],
        'file': 'AAStocks'
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
            'Name',
            'Market',
            'Expected size of primary offer',
            'Expected size of secondary offer',
            'Currency',
            'Price range',
            'Expected first date of trading',
            'Type'
        ],
        'file': 'LSE'
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
            'Code',
            'Abbreviation',
            'Release date',
            'Issue price',
            'Issuance P/E ratio',
            'Total issued',
            'Online release',
            'Number of new shares issued',
            'Number of old shares transferred',
            'Subscription limit',
            'Winning rate (%)',
            'Result of the lottery announcement date',
            'Listing date'
        ],
        'file': 'CNInfo'
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
            'Company Name',
            'Ticker',
            'Date'
        ],
        'file': 'BS-TSX'
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
            'Company Name',
            'Ticker',
            'Date'
        ],
        'file': 'BS-TSXV'
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
            'Date',
            'Company'
        ],
        'file': 'TSX'
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
            'Date',
            'Summary',
            'Market',
            'Sector',
            'Sub Price and Deal Size',
            'First Price and Market Cap'
        ],
        'file': 'Frankfurt'
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
            'Code',
            'Name',
            'Initial listing date',
            'No. of Initial listed shr.(shr.)',
            'Par Value(KRW)',
            'Public Offering Price(KRW)',
            'Industry',
            'Lead Manager'
        ],
        'file': 'KRX'
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
            'Code',
            'Company',
            'Application Date',
            'Amount of Capital (thousand TWD)(while applying for listing)',
            'Date of the Listing Review Committee',
            'Date the application approved by the TWSE Board',
            'Date of the Agreement for Listing submitted to the FSC for recordation',
            'Listing Date',
            'Underwriter',
            'Underwriting price',
            'Note'
        ],
        'file': 'TWSE'
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
            'New Listing Date',
            'ISIN',
            'Bolsa Code',
            'Security',
            'Shares',
            'Nominal',
            'Turnover',
            'Type',
            'Observations'
        ],
        'file': 'BME'
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
            'Company Name',
            'Country of Incorporation',
            'Listing Date',
            'Issue Manager',
            'Offer Price',
            'Amount Raised',
            'IPO Market Cap (mln)',
            '1st Day Closing Price',
            'Against Offer Price Premium /(Discount) $',
            'Closing Price as at Prev Trade Date',
            'Market Cap as at Prev Trade Date (mln)',
            'Trd Dt Against Offer Price Prem./(Discount) $',
            'Listing Board'
        ],
        'file': 'SGX'
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
            'No',
            'Code or Company Name',
            'Name',
            'Listing Date',
            'Delisting Date',
            'IPO (Number of Shares)',
            'Listing Board'
        ],
        'file': 'IDX'
    },
    'BM': {
        'exchange': 'Bursa Malaysia',
        'rank': 25,
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
            'NAME OF COMPANY',
            'OFFER PERIOD Opening',
            'OFFER PERIOD Closing',
            'ISSUE PRICE',
            'NO OF SHARES Public Issue',
            'NO OF SHARES Offer For Sale',
            'NO OF SHARES Private Placement',
            'ISSUING HOUSE / AC NO.',
            'LISTING SOUGHT',
            'DATE OF LISTING (* Tentative)'
        ],
        'file': 'BM'
    },
    'BIT': {
        'exchange': 'Borsa Italiana',
        'rank': None,
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
            'Company Name',
            'Transaction Type',
            'Start of Trading',
            'Market'
        ],
        'file': 'BIT'
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
            'Company',
            'Symbol proposed',
            'Lead Managers',
            'Shares (Millions)',
            'Price Low',
            'Price High',
            'Est. $ Volume',
            'Expected to Trade',
            'SCOOP Rating',
            'Rating Change'
        ],
        'file': 'IPOScoop'
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
            'Company Name',
            'Last',
            'Percent Change',
            'IPO Date'
        ],
        'file': 'NasdaqNordic'
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
            'Subscription Period',
            'Listed',
            'Company',
            'Description',
            'Document'
        ],
        'file': 'Spotlight'
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
        'link_key': 'href',
        'columns': [
            'Symbol',
            'Company Name',
            'Relevant information',
            'Subscription code',
            'Total issuance (ten thousand shares)',
            'Online issuance (10,000 shares)',
            'Top grid subscription needs to be equipped with market value (ten thousand yuan)',
            'Subscription limit (10,000 shares)',
            'Price',
            'Latest price',
            'Close price of the first day',
            'Subscription Date',
            'Announcement date of winning number',
            'Successful payment date',
            'IPO Date',
            'Issue price-earnings ratio',
            'Industry P/E ratio',
            'Success rate (%)',
            'Inquiry Cumulative Quotation Multiple',
            'The number of quotations for allotment objects',
            'Number of consecutive word boards',
            'Increase %',
            'Profit for every first draw (yuan)',
            'Prospectus/Expression of Intent'
        ],
        'file': 'East Money'
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
        json.dump(sources_dict, f)


def read_json(file_name: str = 'sources'):
    with open(file_name + '.json', 'r') as f:
        ex_dict = json.load(f)
    return ex_dict


def check_sources(file_name: str = 'sources'):
    ex_dict = read_json(file_name)
    df = pd.DataFrame(ex_dict).transpose()
    cols = ['exchange', 'rank', 'location', 'url', 'table_num', 'table_elem',  'table_attrs', 'row_elem',
            'cell_elem', 'header_elem', 'columns', 'file']
    df = df[cols]
    if file_name != '':
        df.to_excel(file_name + '.xlsx', index_label='source', freeze_panes=(1, 0))
        df.to_csv(file_name + '.csv', index_label='source')


def main():
    create_json_file()
    check_sources()
    ex_dict = read_json()


if __name__ == '__main__':
    main()
