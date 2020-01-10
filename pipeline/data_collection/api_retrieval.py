import luigi
from pipeline.common.api_calls import ApiCall
from pandas.io.json import json_normalize

_config = luigi.configuration.get_config()


class CryptoWatchResult(ApiCall):
    date_hour = luigi.DateHourParameter()
    name = 'watch'
    path = 'result, rows'
    output_type = 'csv'
    url = _config.get('data collection', 'watch-url')

    def transform_raw(self, data):
        return json_normalize(data)

    def transform(self, df):
        df['hour'] = self.date_hour.hour
        return df
