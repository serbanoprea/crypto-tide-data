import luigi
from datetime import datetime, timedelta

from pipeline.data_collection.api_retrieval import GetApiResult

NOW = datetime.now()
AN_HOUR_AGO = NOW - timedelta(hours=1)


class HourlyCron(luigi.WrapperTask):
    date_hour = luigi.DateHourParameter(default=AN_HOUR_AGO)

    def requires(self):
        yield GetApiResult(**self.param_kwargs)
