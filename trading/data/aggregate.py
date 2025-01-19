from . import nasdaq, macrotrends, yahoo, zacks, seekingalpha
from logging import Logger


# Aggregating methods
def get_shares_outstanding_at(ticker: nasdaq.NasdaqListedEntry, unix_time: float, *, logger: Logger) -> float:
    try:
        return macrotrends.get_shares_outstanding_at(ticker, unix_time, logger=logger)
    except:
        logger and logger.error("Failed to get shares outstanding from macrotrends. Fallback to yahoo.", exc_info=True)
    return float(yahoo.get_shares(ticker.symbol, logger=logger))
def get_first_trade_time(ticker: nasdaq.NasdaqListedEntry, *, logger: Logger = None) -> float:
    return yahoo.get_first_trade_time(ticker.symbol, logger=logger)

def get_hourly_pricing(ticker: nasdaq.NasdaqListedEntry, unix_from: float, unix_to: float, *, logger: Logger = None) -> tuple[list[float], list[float]]:
    return yahoo.get_yahoo_pricing(ticker.symbol, unix_from, unix_to, yahoo.Interval.H1, logger=logger)
def get_daily_pricing(ticker: nasdaq.NasdaqListedEntry, unix_from: float, unix_to: float, *, logger: Logger = None) -> tuple[list[float], list[float]]:
    return yahoo.get_yahoo_pricing(ticker.symbol, unix_from, unix_to, yahoo.Interval.D1, logger=logger)

def get_market_summary(unix_time: float, *, logger: Logger = None) -> str:
    return zacks.get_summary(unix_time, logger = logger)
def get_company_summary(ticker: nasdaq.NasdaqListedEntry, *, logger: Logger = None) -> str:
    try:
        return yahoo.get_summary(ticker.symbol, logger=logger)
    except:
        logger and logger.error(f"Failed to fetch company summary for {ticker}")
        return ""
def get_company_news(ticker: nasdaq.NasdaqListedEntry, unix_from: float, unix_to: float, *, logger: Logger = None) -> str:
    return ". ".join(seekingalpha.get_news(ticker.symbol, unix_from, unix_to, logger = logger)[-7:])