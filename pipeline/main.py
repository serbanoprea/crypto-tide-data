from datetime import datetime, timedelta

import luigi

from pipeline.data_collection.api_retrieval import CryptoWatchResult
from pipeline.data_collection.data_ingress import CryptoWatchDailyIngress
from pipeline.database.database_population import DatabaseHourly, DatabaseDaily

NOW = datetime.now()


class HourlyCron(luigi.WrapperTask):
    date_hour = luigi.DateHourParameter(default=NOW)

    def requires(self):
        yield CryptoWatchResult(**self.param_kwargs)
        yield DatabaseHourly(**self.param_kwargs)


class DailyCron(luigi.WrapperTask):
    date = luigi.DateParameter(default=NOW - timedelta(days=1))

    def requires(self):
        yield CryptoWatchDailyIngress(**self.param_kwargs)
        yield DatabaseDaily(**self.param_kwargs)
