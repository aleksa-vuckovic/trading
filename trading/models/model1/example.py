from ...utils import dateutils
from ...data import aggregate, nasdaq
import random
import time
import numpy as np
from logging import Logger
import logging

START_TIME = dateutils.str_to_unix('2022-07-01 00:00:00', tz = dateutils.EST)
END_TIME = dateutils.str_to_unix('2025-01-01 00:00:00', tz = dateutils.EST)


def generate_input(
    ticker: nasdaq.NasdaqListedEntry,
    prices_size: int,
    prices_ratio: float,
    end_time: float,
    *, logger: Logger = None
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, float, str, str, str, float]:
    #1. Get the prices
    d1_prices_count = int(prices_size*prices_ratio)
    h1_prices_count = prices_size - d1_prices_count
    d1_start_time = (end_time-(d1_prices_count*2+5)*24*3600) #for 10 d1 prices fetch from 20 days earlier
    h1_start_time = (end_time-(h1_prices_count/3+5)*24*3600)

    d1_prices, d1_volumes = aggregate.get_hourly_pricing(ticker, d1_start_time, end_time, logger = logger)
    h1_prices, h1_volumes = aggregate.get_hourly_pricing(ticker, h1_start_time, end_time, logger = logger)
    if not d1_prices or not h1_prices or len(d1_prices) < d1_prices_count or len(h1_prices) < h1_prices_count:
        raise Exception(f'Failed to fetch enough prices for {ticker.symbol}')

    d1_prices = np.array(d1_prices[-d1_prices_count:], dtype=np.float64)
    d1_volumes = np.array(d1_volumes[-d1_prices_count:], dtype=np.float64)
    h1_prices = np.array(h1_prices[-h1_prices_count:], dtype=np.float64)
    h1_volumes = np.array(h1_volumes[-h1_prices_count:], dtype=np.float64)
    
    #2. Normalize prices and volumes
    last_price = d1_prices[-1]
    d1_prices /= last_price
    h1_prices /= last_price

    shares = aggregate.get_shares_outstanding_at(ticker, end_time, logger=logger)
    d1_volumes /= shares
    h1_volumes /= shares
    market_cap = last_price*shares
    
    #3. Get the textual data
    market_summary = aggregate.get_market_summary(end_time)
    company_summary = aggregate.get_company_summary(ticker)
    titles = aggregate.get_company_news(ticker.symbol, d1_start_time, end_time, logger = logger)

    return (d1_prices, d1_volumes, h1_prices, h1_volumes, market_cap, market_summary, company_summary, titles, last_price)

def generate_example(
    ticker: nasdaq.NasdaqListedEntry,
    end_time: float,
    prices_size: int,
    prices_ratio: float = 0.8,

    logger: Logger = None
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, float, str, str, str, float]:
    """
    Generates a text example.
    The total number of prices will be prices_size, with 50:50 divide between 1d and 1h prices.
    The last relevant market summary, the company summary, and the last 10 relevant news titles,
    will be passed as three text segments, to be processed by the language processor separately.
    The expected result is a number between 0 and 1, indicating the price action in the following day of trading (that is 8 hours).
        1 means that the price will rise 5 or more percent.
        0 means that the proce will not rise.
        The inbetween is linearly distibuted.
    Returns:
        (d1_prices, d1_volumes, h1_prices, h1_volumes, market_cap, market_summary, company_summary, titles, expect)
        Where expect is a value from 0 to 1.
    """
    data = generate_input(ticker, prices_size, prices_ratio, end_time, logger=logger)
    last_price = data[-1]
    after_price, _ = aggregate.get_hourly_pricing(ticker.symbol, end_time+60, end_time+5*24*3600, logger=logger)
    if not after_price or len(after_price) < 7:
        raise Exception(f"Failed to fetch enough after prices for {ticker.symbol}")
    after_price = max(after_price[:8]) / last_price
    #4. Get expected result and return
    expect = min(max(after_price - 1, 0), 0.05)*20
    logger.info(f'Generated example for {ticker.symbol} for end time {str(dateutils.unix_to_datetime(end_time))}')
    return data[:-1] + (expect,)

def generate_current(
    ticker: nasdaq.NasdaqListedEntry,
    prices_size: int,
    prices_ratio = 0.8,
    logger: Logger = None
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, float, str, str, str, float]:
    """
    Generates input for prediction.
    Returns: Same as generate_example, except no expected value.
    """
    data = generate_input(ticker, prices_size, prices_ratio, time.time(), logger=logger)
    return data[:-1]
