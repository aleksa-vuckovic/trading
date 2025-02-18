import logging
import time
from ..utils import httputils, dateutils
from ..utils.common import Interval
from . import nasdaq, macrotrends, yahoo, zacks, seekingalpha, globenewswire, wsj, financialtimes

logger = logging.getLogger(__name__)

# Aggregating methods
def get_shares_outstanding_at(ticker: nasdaq.NasdaqListedEntry, unix_time: float) -> float:
    try:
        return macrotrends.get_shares_outstanding_at(ticker, unix_time)
    except:
        pass
    return float(yahoo.get_shares(ticker.symbol))
def get_first_trade_time(ticker: nasdaq.NasdaqListedEntry) -> float:
    return yahoo.get_first_trade_time(ticker.symbol)
def get_market_cap(ticker: nasdaq.NasdaqListedEntry) -> float:
    return yahoo.get_market_cap(ticker.symbol)
def get_sorted_tickers() -> list[nasdaq.NasdaqListedEntry]:
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
    return [it['ticker'] for it in sorted(tickers, key=lambda it: it["unix_time"])]

def get_pricing(ticker: nasdaq.NasdaqListedEntry, unix_from: float, unix_to: float, interval: Interval, return_quotes=['close','volume']) -> tuple[list[float], ...]:
    now = time.time()
    if now-unix_to < 2*24*3600:
        #Live. Use alternatives for recent prices.
        sep = max(now - 3*24*3600, unix_from)
        if unix_from < sep:
            old = yahoo.get_pricing(ticker.symbol, unix_from, sep, interval, return_quotes=return_quotes, backup_behavior=httputils.BackupBehavior.RETHROW|httputils.BackupBehavior.SLEEP)
        else:
            old = None
        try:
            recent = yahoo.get_pricing(ticker.symbol, sep, unix_to, interval, return_quotes=return_quotes)
        except:
            logger.warning(f"Failed live yahoo for {ticker.symbol}.")
            try:
                recent = wsj.get_pricing(ticker.symbol, sep, unix_to, interval, return_quotes=return_quotes)
            except:
                logger.warning(f"Failed wsj for {ticker.symbol}.")
                recent = financialtimes.get_pricing(ticker.symbol, sep, unix_to, interval, return_quotes=return_quotes, backup_behavior=httputils.BackupBehavior.RETHROW|httputils.BackupBehavior.SLEEP)
        if old:
            for i in range(len(recent)):
                old[i].extend(recent[i])
            return old
        else:
            return recent
    else:
        return yahoo.get_pricing(ticker.symbol, unix_from, unix_to, interval, return_quotes=return_quotes, backup_behavior=httputils.BackupBehavior.RETHROW|httputils.BackupBehavior.SLEEP)
def get_interpolated_pricing(ticker: nasdaq.NasdaqListedEntry, unix_from: float, unix_to: float, interval: Interval, return_quotes=['close', 'volume']) -> tuple[list[float], ...]:
    raw_times, *raw_data = get_pricing(ticker, unix_from, unix_to, interval, ['timestamp', *return_quotes])
    if not raw_times:
        return raw_data
    
def get_interpolation_array(timestamps: list[float], interval: Interval):
    pass


def get_market_summary(unix_time: float) -> str:
    return zacks.get_summary(unix_time, backup_behavior=httputils.BackupBehavior.RETHROW|httputils.BackupBehavior.SLEEP)
def get_company_summary(ticker: nasdaq.NasdaqListedEntry) -> str:
    return yahoo.get_summary(ticker.symbol)
def get_company_news(ticker: nasdaq.NasdaqListedEntry, unix_from: float, unix_to: float) -> str:
    try:
        news = seekingalpha.get_news(ticker.symbol, unix_from, unix_to)
    except:
        news = globenewswire.get_news(ticker, unix_from, unix_to, backup_behavior=httputils.BackupBehavior.RETHROW|httputils.BackupBehavior.SLEEP)
    return ". ".join(news[-7:])