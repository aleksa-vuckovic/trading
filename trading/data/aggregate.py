from . import nasdaq, macrotrends, yahoo, zacks, seekingalpha, globenewswire
from ..utils import common
import logging

logger = logging.getLogger(__name__)
_MODULE = __name__.split(".")[-1]
_CACHE = common.CACHE / _MODULE

# Aggregating methods
def get_shares_outstanding_at(ticker: nasdaq.NasdaqListedEntry, unix_time: float) -> float:
    try:
        return macrotrends.get_shares_outstanding_at(ticker, unix_time)
    except:
        pass#logger.error("Failed to get shares outstanding from macrotrends. Fallback to yahoo.", exc_info=True)
    return float(yahoo.get_shares(ticker.symbol))
def get_first_trade_time(ticker: nasdaq.NasdaqListedEntry) -> float:
    return yahoo.get_first_trade_time(ticker.symbol)
def get_sorted_tickers() -> list[dict]:
    tickers = []
    for it in nasdaq.get_filtered_entries():
        try:
            first_trade = get_first_trade_time(it)
        except:
            logger.error(f"Skipping {it.symbol}. No first trade time.")
            continue
        try:
            yahoo.get_shares(it.symbol)
        except:
            logger.error(f"Skipping {it.symbol}. No shares outstanding.")
            continue
        try:
            get_company_summary(it)
        except:
            logger.error(f"Skipping {it.symbol}. No company summary.")
            continue
        tickers.append({"ticker": it, "unix_time": first_trade})
    return sorted(tickers, key=lambda it: it["unix_time"])

def get_hourly_pricing(ticker: nasdaq.NasdaqListedEntry, unix_from: float, unix_to: float) -> tuple[list[float], list[float]]:
    """
    Returns a tuple of arrays - prices and volumes.
    """
    return yahoo.get_yahoo_pricing(ticker.symbol, unix_from, unix_to, yahoo.Interval.H1)
def get_daily_pricing(ticker: nasdaq.NasdaqListedEntry, unix_from: float, unix_to: float) -> tuple[list[float], list[float]]:
    """
    Returns a tuple of arrays - prices and volumes.
    """
    return yahoo.get_yahoo_pricing(ticker.symbol, unix_from, unix_to, yahoo.Interval.D1)

def get_market_summary(unix_time: float) -> str:
    return zacks.get_summary(unix_time)
def get_company_summary(ticker: nasdaq.NasdaqListedEntry) -> str:
    return yahoo.get_summary(ticker.symbol)
def get_company_news(ticker: nasdaq.NasdaqListedEntry, unix_from: float, unix_to: float) -> str:
    try:
        news = seekingalpha.get_news(ticker.symbol, unix_from, unix_to)
    except:
        news = globenewswire.get_news(ticker, unix_from, unix_to)
    return ". ".join(news[-7:])