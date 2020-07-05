import abc
import urllib.request
from datetime import datetime

import luigi
import pandas as pd
from bs4 import BeautifulSoup
from luigi.contrib.s3 import S3Target

from pipeline.common.read import read_sql_df, get_table_columns, read_s3_df
from pipeline.common.tasks import InsertQuery
from pipeline.common.write import s3_write

_config = luigi.configuration.get_config()
_hourly_output_path = _config.get('api-calls', 'hourly-ingress')


class ScrapeTopLevel(luigi.Task):
    date_hour = luigi.DateHourParameter()

    @property
    @abc.abstractmethod
    def source(self):
        pass

    @classmethod
    @abc.abstractmethod
    def url_filter_condition(cls, href):
        pass

    @classmethod
    def transform_content(cls, content):
        return content

    @classmethod
    def format_urls(cls, urls, base_url):
        return list(set([base_url + p.strip('/') if base_url not in p.strip('/') else p.strip('/')
                             for p in urls if cls.url_filter_condition(p)]))

    def scrape_top_level(self, url):
        html = urllib.request.urlopen(url).read()
        parsed = BeautifulSoup(html, features="html.parser")

        for script in parsed(["script", "style", "nav"]):
            script.extract()

        hrefs = parsed.find_all('a')
        partials = [href.get('href') for href in hrefs if href.get('href') is not None and href.get('href') != url]
        filtered = self.format_urls(partials, url)
        not_already_scraped = self._get_not_scraped(filtered)
        return self.scrape_content(not_already_scraped)

    def scrape_content(self, urls):
        df = None

        for url in urls:
            html = urllib.request.urlopen(url).read()

            chunks = self._get_chunks(html)
            row = [url, datetime.now().isoformat(), html, self.transform_content('\n'.join(chunk for chunk in chunks if chunk))]
            data_frame = pd.DataFrame([row], columns=['url', 'scrape_time', 'html', 'content'])
            df = data_frame if df is None else pd.concat([df, data_frame], axis=0)

        return df

    def run(self):
        base_scrape = read_sql_df(
            columns=get_table_columns('BaseScrapes'),
            table='BaseScrapes',
            query="SELECT * FROM {{table}} WHERE ScrapeSource='{source}'".format(source=self.source))

        source, url, subsections = base_scrape.values[0]

        df = self.scrape_top_level(url) if subsections is None else self._scrape_multiple(url, subsections)

        s3_write(df, 'parquet', self.output().path)

    def output(self):
        partial = 'hour={}/{}'.format(self.date_hour.hour, self.source + '.snappy.parquet')
        path = _hourly_output_path.format('scrapes/source=' + self.source, self.date_hour.date()) + partial
        return S3Target(path)

    def read(self):
        read_s3_df(self.output().path, 'parquet')

    def _scrape_multiple(self, url, subsections):
        df = None
        for s in subsections.split(','):
            data_frame = self.scrape_top_level(url.format(s))
            df = data_frame if df is None else pd.concat([df, data_frame], axis=0)

        return df

    def _get_not_scraped(self, urls):
        sql_condition = 'SELECT Url FROM {{table}} WHERE Url IN ({})'.format(",".join(["'{}'".format(u) for u in urls]))

        scraped = read_sql_df(
            columns=['url'],
            table='Scrapes',
            query=sql_condition).values

        vals = [v[0] for v in scraped]

        return [u for u in urls if u not in vals]

    def _get_chunks(self, html):
        parsed = BeautifulSoup(html, features="html.parser")

        for script in parsed(["script", "style"]):
            script.extract()

        text = parsed.get_text()
        lines = (line.strip() for line in text.splitlines())
        return (phrase.strip() for line in lines for phrase in line.split("  "))


class UpdateScrapes(InsertQuery):
    table = 'Scrapes'

    def get_data(self):
        return pd.read_parquet(self.dependency.output().path)[['url', 'scrape_time']]
