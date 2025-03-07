import logging
from urllib import parse
from bs4 import BeautifulSoup
from ..utils import httputils
from ..utils.dateutils import XNAS
from ..data import nasdaq
from .caching import cached_series, CACHE_ROOT, FilePersistor
from .utils import filter_by_timestamp
from .abstract import AbstractSecurity

logger = logging.getLogger(__name__)
_MODULE = __name__.split(".")[-1]
_CACHE = CACHE_ROOT / _MODULE
_BASE_URL = "https://www.globenewswire.com"
def _format_for_url(input: str) -> str:
    input = input.replace('.', '§').replace(',', 'δ')
    return parse.quote(input)

def _get_news_raw(orgs: list[str], keywords: list[str], unix_from: float, unix_to: float) -> list[dict]:
    url = f"{_BASE_URL}/en/search/"
    if orgs:
        url += "organization/"
        for org in orgs: url += f"{_format_for_url(org)}/"
    if keywords:
        url += "keyword/"
        for keyword in keywords: url += f"{_format_for_url(keyword)}/"
    if unix_from and unix_to:
        date_from = XNAS.unix_to_datetime(unix_from).strftime("%Y-%m-%d")
        date_to = XNAS.unix_to_datetime(unix_to + 24*3600 - 0.0001).strftime("%Y-%m-%d")
        url += f"date/[{date_from}%2520TO%2520{date_to}]/"
    url = url[:-1]
    result = []
    page = 1
    while True:
        if page > 1:
            resp = httputils.get_as_browser(f"{url}/load/more", params={'pageSize': 50, 'page': page})
        else:
            resp = httputils.get_as_browser(url, params={'pageSize': 50})
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        divs = soup.find_all("div", class_="pagging-list-item-text-container")
        for div in divs:
            time_span = div.find("span", class_="pagging-list-item-text-date")
            title_link_a = div.find('a', {'data-section': 'article-url'})
            preview_span = div.find("span", class_="pagging-list-item-text-body")
            if time_span and title_link_a:
                unix_time = XNAS.str_to_unix(time_span.text.strip(), format="%B %d, %Y %H:%M ET")
                title = title_link_a.text
                link = title_link_a['href']
                link = f"{_BASE_URL}{link}" if link and link.startswith("/") else link
                preview = preview_span.text.strip() if preview_span else ""
                result.append({'unix_time': unix_time, 'title': title, 'url': link, 'preview': preview})
            else:
                logger.error(f"Failed to parse div:\n{div.decode_contents()}")
        next_div = soup.find('div', class_="pagnition-next")
        if next_div and next_div.find('a'):
            page += 1
        else:
            break
    return result

def _get_org(ticker: AbstractSecurity) -> str:
    try:
        return ticker.name[ticker.name.index(' - ')].strip()
    except:
        return ticker.name
def _get_news_key_fn(security: AbstractSecurity) -> list[str]:
    return [_get_org(security)]
@cached_series(
    unix_args=(1,2),
    key_fn=_get_news_key_fn,
    series_field=None,
    timestamp_field="unix_time",
    persistor_fn=FilePersistor(CACHE_ROOT/_MODULE/'news'),
    time_step_fn=100000000,
    live_delay_fn=3600, #let's say that news is an hour late usually
    live_refresh_fn=2*3600
)
@httputils.backup_timeout()
def _get_news(security: AbstractSecurity, unix_from: float, unix_to: float) -> list[dict]:
    result = _get_news_raw([_get_org(security)], [], unix_from, unix_to)
    result = filter_by_timestamp(result, unix_from=unix_from, unix_to=unix_to, timestamp_field='unix_time')
    return sorted(result, key=lambda it: it['unix_time'])
def get_news(security: AbstractSecurity, unix_from: float, unix_to: float, **kwargs) -> list[str]:
    return [it['title'] for it in _get_news(security, unix_from, unix_to, **kwargs)]

