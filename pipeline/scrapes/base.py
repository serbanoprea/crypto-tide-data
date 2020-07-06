import abc
import urllib.request
import dateutil.parser
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
            row = [url, datetime.now().isoformat(), html,
                   self.transform_content('\n'.join(chunk for chunk in chunks if chunk))]
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
        return pd.read_parquet(self.output().path)

    def on_failure(self, exception):
        df = pd.DataFrame([[None, None, None, None]], columns=['url', 'scrape_time', 'html', 'content'])
        s3_write(df, 'parquet', self.output().path)

    def _scrape_multiple(self, url, subsections):
        df = None
        for s in subsections.split(','):
            data_frame = self.scrape_top_level(url.format(s))
            df = data_frame if df is None else pd.concat([df, data_frame], axis=0)

        return df

    def _get_not_scraped(self, urls):
        cols = get_table_columns('Scrapes')

        scraped = read_sql_df(
            columns=cols,
            table='Scrapes')

        return [u for u in urls if u not in set(scraped['url'])]

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
        df = self.dependency.read()[['url', 'scrape_time']]
        df['formatted_date'] = df['scrape_time'].apply(self._format_date)
        df['scrape_time'] = df['formatted_date']
        return df[['url', 'scrape_time']]

    def _format_date(self, date):
        parsed_date = dateutil.parser.parse(date)
        return '{date:%Y-%m-%d %H:%M:%S}'.format(date=parsed_date)

    def _check_url_presence(self, urls):
        scrapes_table = read_sql_df(
            columns=['url'],
            table='BaseScrapes',
            query='SELECT Url FROM Scrapes WHERE Url IN ({})'.format(','.join(["'{}'".format(url) for url in urls])))

        return len([url for url in urls if url not in scrapes_table['url'].values]) == 0

    def complete(self):
        try:
            df = self.dependency.read()
        except PermissionError:
            return False

        if df.values[0][1] is None:
            return True
        elif df.values[0][1] is not None and not self.output().exists():
            return self._check_url_presence(df['url'].values)
        else:
            return True
