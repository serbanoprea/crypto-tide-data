from pipeline.common.tasks import InsertQuery
from pipeline.data_collection.data_ingress import CryptoWatchHourlyIngress


class InsertCoins(InsertQuery):
    table = 'Coins'

    def transform(self, df):
        return df[['rank', 'name', 'symbol']]

    @property
    def dependency(self):
        return CryptoWatchHourlyIngress(date_hour=self.date_hour)
