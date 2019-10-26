import luigi
from pipeline.common.api_calls import ApiCall

_config = luigi.configuration.get_config()


class GetApiResult(ApiCall):
    date_hour = luigi.DateHourParameter()
    name = 'watch'
    path = 'result, rows'
    output_type = 'json'
    url = _config.get('data collection', 'watch-url')
