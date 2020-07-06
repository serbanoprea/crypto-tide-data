import luigi

from pipeline.scrapes.base import ScrapeTopLevel, UpdateScrapes
from urllib.parse import urlparse


def _clean_content(content):
    chunks = content.split('\n')
    title = chunks[0]
    article = max(chunks, key=len).replace('\xa0', ' ')
    return '\n'.join([title, article])


class ScrapeCoinDesk(ScrapeTopLevel):
    source = 'coindesk'

    def transform_content(self, content):
        return content.replace('24h$0n/aNaN%', '').replace('24h$0NaN%', '')

    @classmethod
    def url_filter_condition(cls, href):
        return href != '/' and '/' not in href[1:] and 'http' not in href and '-' in href


class ScrapeAmbCrypto(ScrapeTopLevel):
    source = 'ambcrypto'

    @classmethod
    def url_filter_condition(cls, href):
        site_specific = ['facebook', 'twitter', 'pinterest', 'instagram', 'youtube', 'linkedin', 'about', 'authors',
                         'job-openings', 'advertise-with-us', 'contact-us', 'terms-and-conditions', 'privacy-policy',
                         'page', '#', 'ambcrypto.com/partners', 'category']
        return href != '/' and not any([p in href for p in site_specific])

    def transform_content(self, content):
        return _clean_content(content)


class ScrapeCoinTelegraph(ScrapeTopLevel):
    source = 'cointelegraph'

    def url_filter_condition(cls, href):
        site_specific = ['price-index', 'authors', 'youtube', 'advertise-with-bitcoins', 'login', 'careers', 'tags', '#']
        return href != '/' and not any([p in href for p in site_specific])

    def format_urls(cls, urls, base_url):
        base = urlparse(base_url)
        return list(set(['{scheme}://{netloc}/{path}'.format(scheme=base.scheme, netloc=base.netloc, path=p.strip('/'))
                         for p in urls if cls.url_filter_condition(p)]))

    def transform_content(self, content):
        return _clean_content(content)


class UpdateCoinDeskScrapes(UpdateScrapes):
    date_hour = luigi.DateHourParameter()

    @property
    def dependency(self):
        return ScrapeCoinDesk(**self.param_kwargs)


class UpdateAmbCryptoScrapes(UpdateScrapes):
    date_hour = luigi.DateHourParameter()

    @property
    def dependency(self):
        return ScrapeAmbCrypto(**self.param_kwargs)


class UpdateScrapeCoinTelegraph(UpdateScrapes):
    date_hour = luigi.DateHourParameter()

    @property
    def dependency(self):
        return ScrapeCoinTelegraph(**self.param_kwargs)


class Scrapes(luigi.WrapperTask):
    date_hour = luigi.DateHourParameter()

    def requires(self):
        yield UpdateCoinDeskScrapes(**self.param_kwargs)
        yield UpdateAmbCryptoScrapes(**self.param_kwargs)
        yield UpdateScrapeCoinTelegraph(**self.param_kwargs)
