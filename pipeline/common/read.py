import re
from urllib import parse

import boto3
import luigi
import pandas as pd
import pyodbc
import s3fs

_tsv = lambda x: pd.read_csv(x, sep='\t', encoding='utf-8')
_csv = lambda x: pd.read_csv(x, encoding='utf-8')
_fs = s3fs.S3FileSystem(anon=False)
_client = boto3.client('s3')

_config = luigi.configuration.get_config()

_db_server = _config.get('database', 'server')
_database = _config.get('database', 'database')
_user = _config.get('database', 'user')
_password = _config.get('database', 'password')
_coins_table = _config.get('database', 'coins-table')


_supported_types = {
    'csv': _csv,
    'tsv': _tsv,
    'json': pd.read_json,
    'parquet': pd.read_parquet
}


def read_s3_df(path, file_format, date=None):
    read_method = _supported_types[file_format]
    if date:
        formatted_date = 'date={:%Y%d%m}'.format(date)
        path = '{}{}/'.format(path, formatted_date)

    if _fs.isfile(path):
        return read_method(path)
    else:
        return _get_df(path, read_method)


def database_connection():
    connection_string = (
            'DRIVER={ODBC Driver 17 for SQL Server}' +
            ';SERVER={server};DATABASE={database};UID={user};PWD={password}'
            .format(server=_db_server, database=_database,
                    user=_user, password=_password)
    )

    return pyodbc.connect(connection_string)


def read_sql_df(columns, table='Coins', query='SELECT * FROM {table}', **kwargs):
    connection = database_connection()
    all_parameters = dict(**{'table': table}, **kwargs)
    query = query.format(**all_parameters)
    all_vals = connection.execute(query).fetchall()
    data = []
    for value in all_vals:
        data.append(dict(zip(columns, value)))

    return pd.DataFrame(data)


def pandas_cols_to_sql(columns):
    cols = []
    _sql_keywords = ['rank', 'symbol']

    for col in columns:
        if ':' in col:
            continue

        if '.' not in col and col.lower() not in _sql_keywords and len(col.split('_')) == 1:
            cols.append(col.capitalize())
        elif len(col.split('_')) > 1:
            cols.append(_multi_word_column(col))
        elif '.' in col or col in _sql_keywords:
            cols.append('[{}]'.format(col.capitalize()))

    return cols


def get_table_columns(table):
    connection = database_connection()
    all_vals = connection.execute('exec sp_columns {table}'.format(table=table)).fetchall()
    return [re.sub(r'(?<!^)(?=[A-Z])', '_', x[3]).lower() for x in all_vals]


def generate_analytics_path(group, name, date, hour=False):
    hourly_template = _config.get('aggregations', 'hourly-output-path')
    daily_template = _config.get('aggregations', 'daily-output-path')
    hourly = not type(hour) == bool
    template = daily_template if not hourly else hourly_template
    return template.format(group, name, date, name) if not hourly else template.format(group, name, date, hour, name)


def _multi_word_column(col):
    return ''.join(word.capitalize() for word in col.split('_'))


def _get_df(path, read_method):
    url_path = parse.urlparse(path)
    objects = _client.list_objects_v2(Bucket=url_path.netloc, Prefix='{}/'.format(url_path.path.strip('/')))
    objects = ['{}://{}/{}'.format(url_path.scheme, url_path.netloc, o['Key']) for o in objects['Contents']]
    df = None
    for path in objects:
        df = read_method(path) if df is None else pd.concat([df, read_method(path)])

    return df


def _chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
