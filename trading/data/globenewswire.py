from urllib import parse
from datetime import datetime
from ..utils import dateutils, common, httputils
from ..data import nasdaq
import requests
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)
_MODULE = __name__.split(".")[-1]
_CACHE = common.CACHE / _MODULE
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
        date_from = dateutils.unix_to_datetime(unix_from, tz=dateutils.ET).strftime("%Y-%m-%d")
        date_to = dateutils.unix_to_datetime(unix_to + 24*3600 - 0.0001, tz=dateutils.ET).strftime("%Y-%m-%d")
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
                unix_time = dateutils.str_to_unix(time_span.text.strip(), format="%B %d, %Y %H:%M ET", tz=dateutils.ET)
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

@common.cached_series(
    unix_from_arg=1,
    unix_to_arg=2,
    include_args=[0],
    cache_root=_CACHE,
    time_step_fn=100000000,
    series_field=None,
    timestamp_field="unix_time",
    live_delay_fn=3600, #let's say that news is an hour late usually
    refresh_delay_fn=2*3600,
    return_series_only=True
)
@common.backup_timeout()
def _get_news(org: str, unix_from: float, unix_to: float) -> list[dict]:
    result = _get_news_raw([org], [], unix_from, unix_to)
    result = reversed(result)
    result = [it for it in result if it['unix_time'] >= unix_from and it['unix_time'] < unix_to]
    return sorted(result, key=lambda it: it['unix_time'])

def get_news(ticker: nasdaq.NasdaqListedEntry, unix_from: float, unix_to: float, **kwargs) -> list[str]:
    return [it['title'] for it in _get_news(ticker.long_name(), unix_from, unix_to, **kwargs)]

