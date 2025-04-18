#2
import logging
from typing import Literal, Sequence, override
from urllib import parse
from bs4 import BeautifulSoup
import config
from base.scraping import scraper, backup_timeout
from base.caching import NullPersistor, Persistor, FilePersistor, SqlitePersistor
from trading.core.securities import Security
from trading.core.news import News, BaseNewsProvider
from trading.providers.utils import filter_news
from trading.providers.nasdaq import Nasdaq

logger = logging.getLogger(__name__)
_MODULE = __name__.split(".")[-1]
_BASE_URL = "https://www.globenewswire.com"
def _format_for_url(input: str) -> str:
    input = input.replace('.', '§').replace(',', 'δ')
    return parse.quote(input)

class GlobeNewswireNews(News):
    def __init__(self, time: float, title: str, content: str, url: str, preview: str):
        super().__init__(time, title, content)
        self.url = url
        self.preview = preview

class GlobeNewswire(BaseNewsProvider):
    def __init__(self, storage: Literal['file','db','none']='db'):
        self.news_persistor = FilePersistor(config.caching.file_path/_MODULE/'news') if storage == 'file'\
            else SqlitePersistor(config.caching.db_path, f"{_MODULE}_news") if storage == 'db'\
            else NullPersistor()

    @backup_timeout()
    def _fetch_news(self,  unix_from: float, unix_to: float, orgs: list[str], keywords: list[str]) -> list[GlobeNewswireNews]:
        url = f"{_BASE_URL}/en/search/"
        if orgs:
            url += "organization/"
            for org in orgs: url += f"{_format_for_url(org)}/"
        if keywords:
            url += "keyword/"
            for keyword in keywords: url += f"{_format_for_url(keyword)}/"
        if unix_from and unix_to:
            date_from = Nasdaq.instance.calendar.unix_to_datetime(unix_from).strftime("%Y-%m-%d")
            date_to = Nasdaq.instance.calendar.unix_to_datetime(unix_to + 24*3600 - 0.0001).strftime("%Y-%m-%d")
            url += f"date/[{date_from}%2520TO%2520{date_to}]/"
        url = url[:-1]
        result: list[GlobeNewswireNews] = []
        page = 1
        while True:
            if page > 1:
                resp = scraper.get(f"{url}/load/more", params={'pageSize': 50, 'page': page})
            else:
                resp = scraper.get(url, params={'pageSize': 50})
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            divs = soup.find_all("div", class_="newsLink")
            for div in divs:
                try:
                    time_span = div.select_one("div.date-source > span")
                    title_link_a = div.select_one("div.mainLink > a")
                    preview_span = div.select_one("div.newsTxt > p")
                    unix_time = Nasdaq.instance.calendar.str_to_unix(time_span.text.strip(), format="%B %d, %Y %H:%M ET")
                    preview = preview_span.text.strip()
                    title = title_link_a.text
                    link: str = title_link_a['href']
                    link = f"{_BASE_URL}{link}" if link.startswith("/") else f"{_BASE_URL}/{link}"
                    article = BeautifulSoup(scraper.get(link).text, "html.parser")
                    article = article.select_one("div.article-body") or article.select_one("div.main-body-container") or article.select_one("body")
                    assert article
                    result.append(GlobeNewswireNews(unix_time, title, article.text, link, preview))
                except:
                    logger.error(f"Failed to parse div:\n{div.decode_contents()}", exc_info=True)
            if soup.select_one('div.pagnition-next > a'): page += 1
            else: break
        return result
    def _get_org(self, security: Security) -> str:
        try:
            return security.name[:security.name.index(' - ')].strip()
        except:
            return security.name

    #region Overrides
    @override
    def get_news_persistor(self, security: Security) -> Persistor:
        return self.news_persistor
    @override
    def get_news_raw(self, unix_from: float, unix_to: float, security: Security) -> Sequence[News]:
        result = self._fetch_news(unix_from, unix_to, [self._get_org(security)], [])
        return filter_news(result, unix_from=unix_from, unix_to=unix_to)
    #endregion