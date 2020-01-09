from urllib import parse

import boto3
import pandas as pd
import s3fs

_tsv = lambda x: pd.read_csv(x, sep='\t', encoding='utf-8')
_csv = lambda x: pd.read_csv(x, encoding='utf-8')
_fs = s3fs.S3FileSystem(anon=False)
_client = boto3.client('s3')


_supported_types = {
    'csv': _csv,
    'tsv': _tsv,
    'json': pd.read_json,
    'parquet': pd.read_parquet
}


def read_s3_df(path, file_format, date=None,):
    prefix = 's3://'
    read_method = _supported_types[file_format]
    if date:
        formatted_date = 'date={:%Y%d%m}'.format(date)
        path = '{}{}/'.format(path, formatted_date)

    if _fs.isfile(path):
        return read_method(path)
    else:
        return _get_df(path, read_method)


def _get_df(path, read_method):
    url_path = parse.urlparse(path)
    objects = _client.list_objects_v2(Bucket=url_path.netloc, Prefix='{}/'.format(url_path.path.strip('/')))
    objects = ['{}://{}/{}'.format(url_path.scheme, url_path.netloc, o['Key']) for o in objects['Contents']]
    df = None
    for paths in _chunk_list(objects, 6):
        df = read_method(*paths) if df is None else pd.concat([df, read_method(*paths)])

    return df


def _chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
