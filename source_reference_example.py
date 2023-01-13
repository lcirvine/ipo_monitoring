import json
import pandas as pd

format_example = \
    {
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


def create_json_file(file_name: str = 'sources'):
    with open(file_name + '.json', 'w') as f:
        json.dump(format_example, f)


def read_json(file_name: str = 'sources'):
    with open(file_name + '.json', 'r') as f:
        ex_dict = json.load(f)
    return ex_dict


def check_sources(file_name: str = 'sources', save_file_as: str = ''):
    ex_dict = read_json(file_name)
    df = pd.DataFrame(ex_dict).transpose()
    cols = ['exchange', 'rank', 'location', 'url', 'table_num', 'table_elem',  'table_attrs', 'row_elem',
            'cell_elem', 'header_elem', 'columns', 'file']
    # cols = ['exchange', 'rank', 'location', 'url', 'table_num', 'table_elem',  'table_attrs', 'row_elem', 'row_attrs',
    #         'cell_elem', 'cell_attrs', 'header_elem', 'header_attrs', 'columns', 'file']
    df = df[cols]
    if save_file_as != '':
        df.to_excel(save_file_as + '.xlsx', index_label='source', freeze_panes=(1, 0))
        df.to_csv(save_file_as + '.csv', index_label='source')


if __name__ == '__main__':
    create_json_file()
    check_sources(save_file_as='sources')
    ex_dict = read_json()
