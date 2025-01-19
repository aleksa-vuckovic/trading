from . import nasdaq, macrotrends, yahoo, zacks, seekingalpha
import logging

logger = logging.getLogger(__name__)

# Aggregating methods
def get_shares_outstanding_at(ticker: nasdaq.NasdaqListedEntry, unix_time: float) -> float:
    try:
        return macrotrends.get_shares_outstanding_at(ticker, unix_time)
    except:
        logger.error("Failed to get shares outstanding from macrotrends. Fallback to yahoo.", exc_info=True)
    return float(yahoo.get_shares(ticker.symbol))
def get_first_trade_time(ticker: nasdaq.NasdaqListedEntry) -> float:
    return yahoo.get_first_trade_time(ticker.symbol)

def get_hourly_pricing(ticker: nasdaq.NasdaqListedEntry, unix_from: float, unix_to: float) -> tuple[list[float], list[float]]:
    return yahoo.get_yahoo_pricing(ticker.symbol, unix_from, unix_to, yahoo.Interval.H1)
def get_daily_pricing(ticker: nasdaq.NasdaqListedEntry, unix_from: float, unix_to: float) -> tuple[list[float], list[float]]:
    return yahoo.get_yahoo_pricing(ticker.symbol, unix_from, unix_to, yahoo.Interval.D1)

def get_market_summary(unix_time: float) -> str:
    return zacks.get_summary(unix_time)
def get_company_summary(ticker: nasdaq.NasdaqListedEntry) -> str:
    try:
        return yahoo.get_summary(ticker.symbol)
    except:
        logger.error(f"Failed to fetch company summary for {ticker}")
        return ""
def get_company_news(ticker: nasdaq.NasdaqListedEntry, unix_from: float, unix_to: float) -> str:
    return ". ".join(seekingalpha.get_news(ticker.symbol, unix_from, unix_to)[-7:])