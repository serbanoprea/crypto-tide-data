import abc

import luigi
from luigi.contrib.s3 import S3Target

from pipeline.common.read import read_s3_df
from pipeline.common.write import s3_write


class ReadableTask(luigi.Task):
    def output(self):
        return S3Target(self._out_path)

    def read(self):
        return read_s3_df(self._out_path, 'parquet')

    @abc.abstractproperty
    def _out_path(self):
        pass

    def write(self, df):
        s3_write(df, 'parquet', self._out_path)
