import json
import logging
from itertools import chain
from typing import Literal, Sequence, TypedDict, override
import config
from base.types import Singleton
from base.caching import cached_scalar, FilePersistor
from base.scraping import backup_timeout, scraper
from base.utils import cached
from trading.core.securities import Exchange, Security, SecurityType, WorkCalendar

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]

class NYSEWorkCalendar(WorkCalendar, Singleton):
    def __init__(self):
        pass
    pass

class NYSESecurity(Security):
    def __init__(self, symbol: str, name: str, type: SecurityType, exchange: Exchange):
        super().__init__(symbol, name, type, exchange)

type _FetchInstrumentType = Literal[
    'COMMON_STOCK', 'UNIT', 'PREFERRED_STOCK', 'CLOSED_END_FUND',
    'EXCHANGE_TRADED_NOTE', 'DEPOSITORY_RECEIPT', 'UNITS_OF_BENEFICIAL_INTEREST',
    'TRUST', 'LIMITED_PARTNERSHIP', 'REIT', 'EXCHANGE_TRADED_FUND'
]
_security_type_map: dict[_FetchInstrumentType, SecurityType] = {
    'COMMON_STOCK': SecurityType.STOCK,
    'PREFERRED_STOCK': SecurityType.STOCK,
    'EXCHANGE_TRADED_FUND': SecurityType.ETF
}
class _FetchResult(TypedDict):
    total: int
    url: str
    exchangeId: str
    instrumentType: str
    symbolTicker: str
    symbolExchangeTicker: str
    normalizedTicker: str
    symbolEsignalTicker: str
    instrumentName: str
    micCode: str
@cached_scalar(
    key_fn=lambda instrumentType: instrumentType,
    persistor_fn=FilePersistor(config.caching.file_path/_MODULE/"listed"),
    refresh_after=7*24*3600
)
@backup_timeout()
def _fetch_listed(instrumentType: Literal['EQUITY', 'EXCHANGE_TRADED_FUND']) -> list[_FetchResult]:
    pageSize = 500
    page = 1
    request = {
        "instrumentType": instrumentType,
        "pageNumber": page,
        "sortColumn": "NORMALIZED_TICKER",
        "sortOrder":"ASC",
        "maxResultsPerPage": pageSize+1,
        "filterToken":""
    }
    url = "https://www.nyse.com/api/quotes/filter"
    data: list[_FetchResult] = json.loads(scraper.post(url, request).text)
    if not data: return []
    total = data[0]["total"]
    while page*pageSize < total:
        page += 1
        request["pageNumber"] = page
        try:
            parsed = json.loads(scraper.post(url, request).text)
            data.extend(parsed)
            logger.info(f"Appended {len(parsed)} entries.")
        except:
            logger.warning(f"Unexpected error when when fetching NYSE stocks.", exc_info=True)
    return data
@cached
def _get_securities() -> Sequence[NYSESecurity]:
    result = []
    total = 0
    for entry in chain(_fetch_listed('EQUITY'), _fetch_listed('EXCHANGE_TRADED_FUND')):
        total += 1
        if entry['instrumentType'] not in _security_type_map: continue
        mic = entry['micCode']
        if mic == NYSE.instance.mic: exchange = NYSE.instance
        elif mic == NYSEArca.instance.mic: exchange = NYSEArca.instance
        elif mic == NYSEAmerican.instance.mic: exchange = NYSEAmerican.instance
        else: continue
        result.append(NYSESecurity(entry['symbolTicker'], entry['instrumentName'], _security_type_map[entry['instrumentType']], exchange))
    logger.info(f"Successfully parsed {len(result)}/{total} securities")
    return result

class NYSE(Exchange):
    def __init__(self):
        super().__init__('XNYS', 'XNYS', 'XNYS', 'NYSE', NYSEWorkCalendar.instance)
    @override
    @cached
    def securities(self) -> Sequence[NYSESecurity]:
        return [it for it in _get_securities() if it.exchange == NYSE.instance]

class NYSEAmerican(Exchange):
    def __init__(self):
        super().__init__('XASE', 'XASE', 'XNYS', 'NYSE', NYSEWorkCalendar.instance)
    @override
    @cached
    def securities(self) -> Sequence[NYSESecurity]:
        return [it for it in _get_securities() if it.exchange == NYSEAmerican.instance]
    
class NYSEArca(Exchange):
    def __init__(self):
        super().__init__('ARCX', 'ARCX', 'XNYS', 'NYSE', NYSEWorkCalendar.instance)
    @override
    @cached
    def securities(self) -> Sequence[NYSESecurity]:
        return [it for it in _get_securities() if it.exchange == NYSEArca.instance]
