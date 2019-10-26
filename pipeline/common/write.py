from collections import namedtuple
from io import StringIO, BytesIO

import boto3
import urllib3

_string_write_formats = {
    'csv': lambda df, buffer: df.to_csv(buffer),
    'json': lambda df, buffer: df.to_json(buffer),
}

_binary_write_formats = {
    'csv': lambda df, buffer: df.to_csv(buffer),
}


def _get_bucket_key(path):
    url = urllib3.util.parse_url(path)
    BucketKey = namedtuple('BucketAndKey', ['bucket', 'object_key'])
    return BucketKey(url.netloc, url.path[1:])


def s3_write(df, output_format, path):
    if output_format not in _string_write_formats.keys():
        raise WriteException('Write format not supported')

    if output_format in _string_write_formats.keys():
        buffer = StringIO()
    else:
        buffer = BytesIO()

    _string_write_formats[output_format](df, buffer)
    s3_resource = boto3.resource('s3')
    url = _get_bucket_key(path)
    s3_resource.Object(url.bucket, url.object_key).put(Body=buffer.getvalue())


class WriteException(Exception):
    pass
