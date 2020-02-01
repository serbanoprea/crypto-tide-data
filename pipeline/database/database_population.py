import luigi

from pipeline.common.read import read_sql_df
from pipeline.common.tasks import DatabaseQuery, InsertQuery
from pipeline.data_collection.data_ingress import CryptoWatchHourlyIngress

_config = luigi.configuration.get_config()
_coins_table = _config.get('database', 'coins-table')
_values_table = _config.get('database', 'values-table')


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
        for row in self.get_data().values:
            rank, _, symbol = row
            yield (
                "UPDATE {table} SET Rank={current_rank} WHERE Symbol='{symbol}'"
                .format(table=self.table, current_rank=rank, symbol=symbol)
            )

    def requires(self):
        return CryptoWatchHourlyIngress(date_hour=self.date_hour)


class InsertHourlyValues(InsertQuery):
    date_hour = luigi.DateHourParameter()

    table = _values_table

    def transform(self, df):
        coins = read_sql_df(_coins_table, ['coin_id', 'rank', 'previous_rank', 'symbol', 'name'])
        df = df[[c for c in df.columns if c not in ['name', 'id'] and ':' not in c]]
        complete_dataset = df.merge(coins[['symbol', 'coin_id']], on='symbol', how='inner')
        complete_dataset['Date'] = '{:%Y-%m-%d}'.format(self.date_hour.date())
        return complete_dataset

    @property
    def dependency(self):
        return CryptoWatchHourlyIngress(date_hour=self.date_hour)
