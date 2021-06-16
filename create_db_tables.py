import os
import sys
import pandas as pd
from source_reference import return_sources
from pg_connection import pg_connection, convert_cols_db
from sqlalchemy import types as sql_types

conn = pg_connection()


def source_raw_tables():
    sources_dict = return_sources(source_type='all')
    source_folder = os.path.join(os.getcwd(), 'Data from Sources')
    for source, details in sources_dict.items():
        df = pd.read_csv(os.path.join(source_folder, details.get('file') + '.csv'))
        df.rename(columns={'time_checked': 'time_added'}, inplace=True)
        df['time_removed'] = pd.NaT
        for col in ['time_added', 'time_removed']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col].fillna(pd.NaT), errors='coerce')
        tbl = details.get('db_table_raw')
        df.to_sql(tbl, conn, if_exists='replace', index=False,
                  dtype={
                      'time_added': sql_types.DateTime,
                      'time_removed': sql_types.DateTime
                  })


def entity_mapping_table():
    df = pd.read_excel(os.path.join('Reference', 'Entity Mapping.xlsx'))
    df.columns = convert_cols_db(df.columns)
    df.to_sql('entity_mapping', conn, if_exists='replace', index=False)


def peo_pipe_table():
    df = pd.read_excel(os.path.join('Reference', 'PEO-PIPE IPO Data.xlsx'))
    df.columns = df.columns = convert_cols_db(df.columns)
    df.to_sql('peo_pipe', conn, if_exists='replace', index=False)


def comparison_table():
    df = pd.read_excel(os.path.join('Results', 'IPO Monitoring.xlsx'), sheet_name='Comparison')
    df.columns = df.columns = convert_cols_db(df.columns)
    df.to_sql('comparison', conn, if_exists='replace', index=False)


def webscraping_results():
    df_all = pd.read_csv(os.path.join('Logs', 'Webscraping Results.csv'))
    df_all.columns = df_all.columns = convert_cols_db(df_all.columns)
    df_all.to_sql('webscraping_results', conn, if_exists='replace', index=False)

    df = pd.read_csv(os.path.join('Logs', 'Recent Webscraping Performance.csv'))
    df.columns = df.columns = convert_cols_db(df.columns)
    df.to_sql('webscraping_results_recent', conn, if_exists='replace', index=False)


def rpd_table():
    df = pd.read_excel(os.path.join('Reference', 'IPO Monitoring RPDs.xlsx'))
    df.columns = df.columns = convert_cols_db(df.columns)
    df.to_sql('rpd_ipo_monitoring', conn, if_exists='replace', index=False)


if __name__ == '__main__':
    try:
        rpd_table()
    except Exception as e:
        print(e, sys.exc_info())
    finally:
        conn.close()
