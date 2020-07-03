import abc

import luigi
import pandas as pd
import requests
from luigi.contrib.s3 import S3Target

from pipeline.common.write import s3_write
from datetime import datetime

_config = luigi.configuration.get_config()
_output_path = _config.get('api-calls', 'path')


class ApiCall(luigi.Task):
    @abc.abstractproperty
    def url(self):
        pass

    @abc.abstractproperty
    def name(self):
        pass

    output_type = 'parquet'

    @classmethod
    def transform(cls, df):
        '''
        Used to transform the form of the data
        :param df: Pandas dataframe
        :return: Transformed pandas dataframe
        '''
        return df

    @classmethod
    def transform_raw(cls, data):
        return data

    def run(self):
        data = self._read_data()
        self.write(data)

    def write(self, data):
        s3_write(data, self.output_type, self._out_path)

    def output(self):
        return S3Target(self._out_path)

    def complete(self):
        current = datetime.now()
        current_date_hour = datetime(year=current.year, month=current.month, day=current.day, hour=current.hour)

        if (current_date_hour - self.date_hour).seconds == 0 and not self.output().exists():
            return False
        else:
            return True

    @property
    def _out_path(self):
        path = _output_path.format(self.name, self.date_hour.date(), self.date_hour.hour)
        return '{}/{}.{}'.format(path, self.name, self.output_type)

    def _get_format(self):
        if hasattr(self, 'format'):
            return self.format
        else:
            return 'json'

    def _get_response(self, result):
        if hasattr(self, 'path'):
            for fragment in self.path.split(', '):
                result = result[fragment]

        return result

    def _read_data(self):
        read_methods = {
            'csv': pd.read_csv,
            'tsv': lambda x: pd.read_csv(x, sep='\t')
        }

        api_return_format = self._get_format()

        if api_return_format == 'json':
            response = requests.get(self.url).json()
            response = self._get_response(response)
            response = self.transform_raw(response)

            return self.transform(pd.DataFrame(response))

        if api_return_format not in read_methods.keys():
            raise Exception('Please provide a valid API format')

        return read_methods[api_return_format](self.url)
