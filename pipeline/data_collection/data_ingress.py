import abc
from datetime import datetime, time, timedelta

import luigi
from luigi.contrib.s3 import S3Target
from luigi.tools.range import RangeHourly

from pipeline.common.read import read_s3_df
from pipeline.common.write import s3_write
from pipeline.data_collection.api_retrieval import CryptoWatchResult

_config = luigi.configuration.get_config()
_hourly_output_path = _config.get('api-calls', 'hourly-ingress')
_daily_output_path = _config.get('api-calls', 'daily-ingress')


class _ReadableTask(luigi.Task):
    def output(self):
        return S3Target(self._out_path)

    def read(self):
        return read_s3_df(self._out_path, 'parquet')

    @abc.abstractproperty
    def _out_path(self):
        pass

    def write(self, df):
        s3_write(df, 'parquet', self._out_path)


class HourlyIngress(_ReadableTask):
    date_hour = luigi.DateHourParameter()

    @abc.abstractproperty
    def name(self):
        pass

    @property
    def _out_path(self):
        return (
            _hourly_output_path
            .format(self.name, self.date_hour.date()) +
            'hour={}/{}.snappy.parquet'.format(self.date_hour.hour, self.name)
        )



class DailyIngress(_ReadableTask):
    date = luigi.DateParameter()

    @abc.abstractproperty
    def dependency(self):
        pass

    @abc.abstractproperty
    def name(self):
        pass

    def requires(self):
        return RangeHourly(
            of=self.dependency,
            start=datetime.combine(self.date, time.min),
            stop=datetime.combine(self.date + timedelta(days=1), time.min)
        )

    def output(self):
        return S3Target(self._out_path)

    @property
    def _out_path(self):
        return _daily_output_path.format(self.name, self.date, self.name)


class CryptoWatchHourlyIngress(HourlyIngress):
    name = 'watch'

    def requires(self):
        return CryptoWatchResult(**self.param_kwargs)

    def run(self):
        df = read_s3_df(self.input().path, 'csv')
        self.write(df)


class CryptoWatchDailyIngress(DailyIngress):
    dependency = CryptoWatchHourlyIngress
    name = 'watch'

    def run(self):
        df = read_s3_df(_hourly_output_path.format(CryptoWatchHourlyIngress.name, self.date), 'parquet')
        self.write(df)
