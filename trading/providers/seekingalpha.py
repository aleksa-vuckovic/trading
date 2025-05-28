#2
import json
import logging
from typing import TypedDict, override
from datetime import datetime
from bs4 import BeautifulSoup
from base.scraping import scraper, backup_timeout
from trading.core.securities import Security
from trading.core.news import BaseNewsProvider, News
from trading.providers.forex import ForexSecurity
from trading.providers.nasdaq import NasdaqSecurity
from trading.providers.nyse import NYSESecurity
from trading.providers.utils import filter_news

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]

def _get_symbol(security: Security):
    if isinstance(security, (NasdaqSecurity, NYSESecurity)):
        return security.symbol
    if isinstance(security, ForexSecurity):
        return f"{security.base.lower()}.{security.quote.lower()}"
    raise Exception(f"Unsupported security {security}.")

class _NewsAtrributes(TypedDict):
    publishOn: str
    isLockedPro: bool
    commentCount: int
    title: str
    isPaywalled: bool
class _NewsLinks(TypedDict):
    self: str
class _NewsResponse(TypedDict):
    attributes: _NewsAtrributes
    links: _NewsLinks

_BASE_URL = "https://seekingalpha.com"
_COOKIES = {'_px3': 'af68f8fb5b5ccac7de1bcd1e9557a64ca5b54a306698c0183f442b892e57d595:dKFZkKZdn5YMhnVe9/z6Mmeu0Vwm9hHxkSUA12xuWMNzrzBEhvWN72yAkcs/21uiP2FNRzAYmXdyl1IhWgTqig==:1000:XTwygXvwwmUUlYgT7A6afuaPKRfJfP1V/ubqBfweaOzdYj2FzrwVIEpS1yYWXwHLg2Hdu8M4pe1w6fKMCzre5OEMW3L/9/vFIfRzFIQtOXysmQuLcIU33BSX4fmq4wtEMaSHXZ70WNml3AY90zQCEDe/xiIN0Q6JcQNtnFJzkmwCeU3vqCzGsti1r4SuxMkeyr0MVTrVnwWee1wEWlPGdeB57SWQIZ+Dt71z86HzN7I='}
class SeekingAlpha(BaseNewsProvider):

    @backup_timeout()
    def _fetch_news(self, unix_from: float, unix_to: float, symbol: str) -> list[News]:
        url = f"{_BASE_URL}/api/v3/symbols/{symbol}/news?filter[since]={int(unix_from-1000)}&filter[until]={int(unix_to+1000)}&id={symbol}&include=author&isMounting=true&page[size]=50&page[number]="
        i = 1
        ret: list[News] = []
        while True:
            result = scraper.get(url + str(i), cookies=_COOKIES)
            data: list[_NewsResponse] = json.loads(result.text)['data']
            for item in data:
                try:
                    att = item['attributes']
                    if not att['isLockedPro'] and not att['isPaywalled'] and 'self' in item['links']:
                        try:
                            link = f"{_BASE_URL}{item['links']['self']}"
                            tmp = scraper.get(link, cookies=_COOKIES)
                            content_div = BeautifulSoup(tmp.text, "html.parser").find("div", attrs={"data-test-id": "content-container"})
                            assert content_div
                            content = content_div.text
                        except:
                            logger.warning(f"Failed to fetch news content from {item['links']}", exc_info=True)
                            content = None
                    else: content = None
                    ret.append(News(datetime.fromisoformat(att['publishOn']).timestamp(), att['title'], content))
                except:
                    logger.warning(f"Failed to parse news item: {item}.", exc_info=True)
                    pass
            if len(data) < 50:
                break
            i += 1
        return ret
    
    #region Overrides
    @override
    def get_news_raw(self, unix_from: float, unix_to: float, security: Security) -> list[News]:
        return filter_news(self._fetch_news(unix_from, unix_to, _get_symbol(security)), unix_from, unix_to)
    #endregion