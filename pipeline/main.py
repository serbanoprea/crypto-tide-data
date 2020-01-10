from datetime import datetime

import luigi

from pipeline.data_collection.api_retrieval import CryptoWatchResult

NOW = datetime.now()


class HourlyCron(luigi.WrapperTask):
    date_hour = luigi.DateHourParameter(default=NOW)

    def requires(self):
        yield CryptoWatchResult(**self.param_kwargs)
