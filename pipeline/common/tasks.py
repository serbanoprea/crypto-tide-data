import abc
import re
import types

import luigi
import numpy as np
from luigi.contrib.s3 import S3Target, S3FlagTarget

from pipeline.common.read import read_s3_df, database_connection, pandas_cols_to_sql
from pipeline.common.write import s3_write, mark_success

_config = luigi.configuration.get_config()

_hourly_success_token_path = _config.get('misc', 'hourly-success-tokens')
_daily_success_token_path = _config.get('misc', 'daily-success-tokens')

_sql_keywords = ['rank', 'symbol']


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


class DatabaseQuery(luigi.Task):
    number_of_batches = 10

    @abc.abstractproperty
    def sql(self):
        pass

    @property
    def connection(self):
        return database_connection()

    @classmethod
    def transform(cls, df):
        return df

    @staticmethod
    def _list_to_insert(in_list):
        return tuple(in_list).__repr__()

    def run(self):
        sql = self.sql
        if type(sql) is types.GeneratorType:
            for cmd in sql:
                self.connection.execute(cmd).commit()
        else:
            self.connection.execute(sql).commit()

        mark_success(self._out_path)

    def output(self):
        return S3FlagTarget(self._out_path)

    def get_data(self):
        data = self.requires().read()
        return self.transform(data)

    @property
    def _out_path(self):
        pattern = re.compile('^(.*?)\(')
        name = pattern.match(str(self))[0].strip('(')

        if hasattr(self, 'date_hour'):
            return _hourly_success_token_path.format(name, self.date_hour.date(), self.date_hour.hour)
        else:
            return _daily_success_token_path.format(name, self.date)


class InsertQuery(DatabaseQuery):
    @abc.abstractproperty
    def table(self):
        pass

    @abc.abstractproperty
    def dependency(self):
        pass

    @property
    def sql(self):
        data = self.get_data()
        columns = pandas_cols_to_sql(data.columns)
        format_columns = '({})'.format(','.join(columns)).strip(',')
        template = 'INSERT INTO {table} {columns} VALUES'.format(table=self.table,
                                                                 columns=format_columns)
        for batch in np.array_split(data[[c for c in data.columns if ':' not in c]].values, self.number_of_batches):
            yield '{template} {values}'.format(
                template=template,
                values=','.join(self._list_to_insert(row) for row in batch).strip(',')
            )

    def requires(self):
        return self.dependency


class TruncateTableQuery(DatabaseQuery):
    @abc.abstractproperty
    def table(self):
        pass

    @property
    def sql(self):
        return 'DELETE FROM {table}'.format(table=self.table)
