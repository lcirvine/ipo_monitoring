import os
import re
import configparser
from sqlalchemy import create_engine
from typing import Union
from pandas.core.indexes.base import Index
from sqlalchemy import types as sql_types
# import psycopg2


def convert_cols_db(col_list: Union[list, Index]) -> list:
    rc = re.compile(r'([\(\)\?\+]*)')
    return [rc.sub('', col).lower().replace(' ', '_') for col in col_list]


def pg_connection(database: str = 'ipo_monitoring'):
    parent_folder = os.path.dirname(os.getcwd())
    pg_config = configparser.ConfigParser()
    pg_config.read(os.path.join(parent_folder, 'postgres_db.ini'))
    un = pg_config.get(database, 'user')
    pw = pg_config.get(database, 'password')
    host = pg_config.get(database, 'host')
    db = pg_config.get(database, 'database')
    engine = create_engine(f"postgresql+psycopg2://{un}:{pw}@{host}:5432/{db}")
    return engine.connect()
