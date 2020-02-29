from datetime import timedelta, datetime, time

import luigi
import numpy as np
from luigi.tools.range import RangeHourly

from pipeline.common.read import read_sql_df
from pipeline.common.tasks import DatabaseQuery, InsertQuery
from pipeline.data_collection.data_ingress import CryptoWatchHourlyIngress

_config = luigi.configuration.get_config()
_coins_table = _config.get('database', 'coins-table')
_values_table = _config.get('database', 'values-table')
_window_period = _config.getint('database', 'window-period')


class UpdateCoinsRank(DatabaseQuery):
    date_hour = luigi.DateHourParameter()
    table = _coins_table

    @property
    def sql(self):
        yield (
            'UPDATE {table} SET {previous_rank}={current_rank}'
            .format(table=self.table, previous_rank='PreviousRank', current_rank='Rank')
        )

        for query in self._update_current_rank:
            yield query

    def transform(self, df):
        return df[['rank', 'name', 'symbol']]

    @property
    def _update_current_rank(self):
        for batch in np.array_split(self.get_data().values, self.number_of_batches):
            update_str = ''
            for row in batch:
                rank, _, symbol = row
                update_str += (
                    "UPDATE {table} SET Rank={current_rank} WHERE Symbol='{symbol}';\n"
                    .format(table=self.table, current_rank=rank, symbol=symbol)
                )

            yield update_str

    def requires(self):
        return CryptoWatchHourlyIngress(date_hour=self.date_hour)


class HourlyValuesCleanup(DatabaseQuery):
    date_hour = luigi.DateHourParameter()

    @property
    def sql(self):
        date_limit = self.date_hour.date() - timedelta(days=_window_period)
        return (
            "DELETE FROM {table} WHERE Date <= CONVERT(DATE, '{date:%Y-%m-%d}') AND Hour <= {hour}"
            .format(table=_values_table, date=date_limit, hour=self.date_hour.hour)
        )


class InsertHourlyValues(InsertQuery):
    date_hour = luigi.DateHourParameter()

    table = _values_table

    def transform(self, df):
        coins = read_sql_df(['coin_id', 'rank', 'previous_rank', 'symbol', 'name'], table=_coins_table)
        complete_dataset = df.merge(coins[['symbol', 'coin_id']], on='symbol', how='inner')
        complete_dataset = complete_dataset.drop(['level_0', 'Unnamed: 0', 'index', 'name', 'id'], axis=1)

        complete_dataset['Date'] = '{:%Y-%m-%d}'.format(self.date_hour.date())
        return complete_dataset

    @property
    def dependency(self):
        return CryptoWatchHourlyIngress(date_hour=self.date_hour)


class DatabaseHourly(luigi.WrapperTask):
    date_hour = luigi.DateHourParameter()

    def requires(self):
        yield InsertHourlyValues(**self.param_kwargs)
        yield HourlyValuesCleanup(**self.param_kwargs)
        yield UpdateCoinsRank(**self.param_kwargs)


class DatabaseDaily(luigi.WrapperTask):
    date = luigi.DateParameter()

    def requires(self):
        return RangeHourly(
            of=DatabaseHourly,
            start=datetime.combine(self.date, time.min),
            stop=datetime.combine(self.date + timedelta(days=1), time.min)
        )
