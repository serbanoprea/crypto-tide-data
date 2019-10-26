import luigi
from datetime import datetime, timedelta

from pipeline.data_collection.api_retrieval import GetApiResult

NOW = datetime.now()


class HourlyCron(luigi.WrapperTask):
    date_hour = luigi.DateHourParameter(default=NOW)

    def requires(self):
        yield GetApiResult(**self.param_kwargs)
