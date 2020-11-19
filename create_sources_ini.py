import os
import pandas as pd
import configparser


def create_ini(file: str, section: str, result_file: str, columns: list = None):
    config = configparser.ConfigParser()
    file_ext = os.path.splitext(file)[1]
    assert file_ext in ['.xlsx', '.csv'], f'{file_ext} is not a supported file type. Please use .xlsx or .csv'
    if file_ext == '.xlsx':
        df = pd.read_excel(file, na_filter=False)
    else:
        df = pd.read_csv(file, na_filter=False)
    if columns:
        df = df[columns]
    for index, row in df.iterrows():
        config[row[section]] = {col: row[col] for col in df.columns.to_list() if col != section}
    with open(result_file + '.ini', 'w') as f:
        config.write(f)


if __name__ == '__main__':
    file_name = os.path.join(os.getcwd(), 'Exchanges.csv')
    cols = ['source', 'exchange', 'url', 'elem', 'attr_key', 'attr_value', 'file']
    create_ini(file_name, 'source', 'sources')
