import luigi

from pipeline.common.tasks import DatabaseQuery
from pipeline.data_collection.data_ingress import CryptoWatchHourlyIngress

_config = luigi.configuration.get_config()
_coins_table = _config.get('database', 'coins-table')


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
                'UPDATE {table} SET Rank={current_rank} WHERE Symbol={symbol}'
                .format(table=self.table, current_rank=rank, symbol=symbol)
            )

    def requires(self):
        return CryptoWatchHourlyIngress(date_hour=self.date_hour)
