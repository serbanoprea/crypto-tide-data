from datetime import datetime, timedelta

import luigi

from pipeline.data_aggregation.output_db_aggregates import DailyDbAggregatesOutput, HourlyDbAggregatesOutput
from pipeline.data_collection.api_retrieval import CryptoWatchResult
from pipeline.data_collection.data_ingress import CryptoWatchDailyIngress
from pipeline.database.aggregates_insert import InsertPopulationAggregates, InsertCoinAggregates
from pipeline.database.database_population import DatabaseHourly, DatabaseDaily
from pipeline.database.trends_insert import InsertDailyTrends, InsertHourlyTrends

NOW = datetime.now()


class HourlyCron(luigi.WrapperTask):
    date_hour = luigi.DateHourParameter()

    def requires(self):
        yield CryptoWatchResult(**self.param_kwargs)
        yield DatabaseHourly(**self.param_kwargs)
        yield InsertHourlyTrends(**self.param_kwargs)
        yield InsertCoinAggregates(**self.param_kwargs)
        yield HourlyDbAggregatesOutput(**self.param_kwargs)


class DailyCron(luigi.WrapperTask):
    date = luigi.DateParameter()

    def requires(self):
        yield CryptoWatchDailyIngress(**self.param_kwargs)
        yield DatabaseDaily(**self.param_kwargs)
        yield InsertDailyTrends(**self.param_kwargs)
        yield InsertPopulationAggregates(**self.param_kwargs)
        yield DailyDbAggregatesOutput(**self.param_kwargs)


class CryptoTideCron(luigi.WrapperTask):
    def requires(self):
        yield HourlyCron(date_hour=NOW)
        yield DailyCron(date=NOW - timedelta(days=1))
