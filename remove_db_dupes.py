import os
import pandas as pd
import json
from pg_connection import pg_connection, sql_types

sources_file = os.path.join(os.getcwd(), 'sources.json')
if os.path.exists(sources_file):
    with open(sources_file, 'r') as f:
        sources = json.load(f)

conn = pg_connection()

tables_with_dupes = {
    'source_asx_raw': {
        'time_added': sql_types.DateTime,
        'time_removed': sql_types.DateTime,
        'ipo_date': sql_types.DATE
    },
    'source_tkipo_raw': {
        'time_added': sql_types.DateTime,
        'time_removed': sql_types.DateTime,
        'ipo_date': sql_types.DATE,
        'ticker': sql_types.BIGINT,
        'price': sql_types.FLOAT
    },
    'source_ipohub_raw': {
        'time_added': sql_types.DateTime,
        'time_removed': sql_types.DateTime,
        'ipo_date': sql_types.DATE,
        'price': sql_types.FLOAT
    },
    'source_ipohub': {
        'time_added': sql_types.DateTime,
        'time_removed': sql_types.DateTime,
        'ipo_date': sql_types.DATE,
        'price': sql_types.FLOAT
    }
}

for tbl, dt in tables_with_dupes.items():
    df = pd.read_sql_table(tbl, conn)
    len_original = len(df)
    df.drop_duplicates(inplace=True)
    len_new = len(df)
    if len_new != len_original:
        print(f"{tbl} - {len_original - len_new} duplicate rows, {len_new} rows remain")
        df.to_sql(tbl, conn, if_exists='replace', index=False, dtype=dt)

conn.close()
