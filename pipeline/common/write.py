from collections import namedtuple
from io import StringIO, BytesIO
from datetime import datetime, date

import boto3
import luigi
import urllib3

_s3_resource = boto3.resource('s3')

_string_write_formats = {
    'csv': lambda df, buffer: df.to_csv(buffer),
    'json': lambda df, buffer: df.to_json(buffer),
}

_binary_write_formats = {
    'parquet': lambda df, buffer: df.to_parquet(buffer, compression='snappy'),
}


def _get_bucket_key(path):
    url = urllib3.util.parse_url(path)
    BucketKey = namedtuple('BucketAndKey', ['bucket', 'object_key'])
    return BucketKey(url.netloc, url.path[1:])


def s3_write(df, output_format, path):
    combined_write_formats = dict(**_string_write_formats, **_binary_write_formats)

    if output_format not in combined_write_formats.keys():
        raise WriteException('Write format not supported')

    if output_format in _string_write_formats.keys():
        buffer = StringIO()
    else:
        buffer = BytesIO()

    combined_write_formats[output_format](df, buffer)
    bucket, key = _get_bucket_key(path)
    _s3_resource.Object(bucket, key).put(Body=buffer.getvalue())


def mark_success(path):
    bucket, key = _get_bucket_key(path)
    key += '_SUCCESS'
    buffer = StringIO('')
    _s3_resource.Object(bucket, key).put(buffer.getvalue())


class WriteException(Exception):
    pass
