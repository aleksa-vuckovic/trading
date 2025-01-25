from ...utils import dateutils
from ...data import aggregate, nasdaq
import random
import time
import numpy as np
import logging
import torch
from torch.autograd import Function
from transformers import BertTokenizer, BertModel
import math

logger = logging.getLogger(__name__)

tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
model = BertModel.from_pretrained('bert-base-uncased')
TEXT_EMBEDDING_SIZE = 768
D1_PRICES = 700
H1_PRICES = 300
D1_PRICES_I = 0
D1_VOLUMES_I = D1_PRICES_I + D1_PRICES
H1_PRICES_I = D1_VOLUMES_I + D1_PRICES
H1_VOLUMES_I = H1_PRICES_I + H1_PRICES
TEXT1_I = H1_VOLUMES_I + H1_PRICES
TEXT2_I = TEXT1_I + TEXT_EMBEDDING_SIZE
TEXT3_I = TEXT2_I + TEXT_EMBEDDING_SIZE
MARKET_CAP_I = TEXT3_I + TEXT_EMBEDDING_SIZE
LIN_5_RESULT_I = MARKET_CAP_I+1
LIN_10_RESULT_I = LIN_5_RESULT_I + 1
LIN_5_5_RESULT_I = LIN_10_RESULT_I + 1
LIN_10_10_RESULT_I = LIN_5_5_RESULT_I + 1
SIG_5_RESULT_I = LIN_10_10_RESULT_I + 1
SIG_10_RESULT_I = SIG_5_RESULT_I + 1
TAN_5_RESULT_I = SIG_10_RESULT_I + 1
TAN_10_RESULT_I = TAN_5_RESULT_I + 1

result_types = [
    LIN_5_RESULT_I,
    LIN_10_RESULT_I,
    LIN_5_5_RESULT_I,
    LIN_10_10_RESULT_I,
    SIG_5_RESULT_I,
    SIG_10_RESULT_I,
    TAN_5_RESULT_I,
    TAN_10_RESULT_I
]

def bounded(value: float, low: float, high: float):
    return min(max(value, low), high)
def get_result(type: int, price_difference: float) -> float:
    x = price_difference
    if type == LIN_5_RESULT_I:
        return bounded(x, 0, 0.05)/0.05
    if type == LIN_10_RESULT_I:
        return bounded(x, 0, 0.1)/0.1
    if type == LIN_5_5_RESULT_I:
        return bounded(x, -0.05, 0.05)/0.05
    if type == LIN_10_10_RESULT_I:
        return bounded(x, -0.1, 0.1)/0.1
    if type == SIG_5_RESULT_I:
        t = math.e**(300*x-6)
        return t/(1+t)
    if type == SIG_10_RESULT_I:
        t = math.e**(150*x-7.5)
        return t/(1+t)
    if type == TAN_5_RESULT_I:
        t = math.e**(-150*x)
        return (1-t)/(1+t)
    if type == TAN_10_RESULT_I:
        t = math.e**(-6*x)
        return (1-t)/(1+t)
    raise Exception("Unknown result type")

def generate_input(
    ticker: nasdaq.NasdaqListedEntry,
    end_time: float
) -> tuple[torch.Tensor, float]:
    #1. Get the prices
    d1_start_time = (end_time-(D1_PRICES/5*7*1.2+5)*24*3600)
    h1_start_time = (end_time-(H1_PRICES/6/5*7*1.5+5)*24*3600)

    d1_prices, d1_volumes = aggregate.get_daily_pricing(ticker, d1_start_time, end_time)
    h1_prices, h1_volumes = aggregate.get_hourly_pricing(ticker, h1_start_time, end_time)
    if not d1_prices or len(d1_prices) < D1_PRICES:
        raise Exception(f'Failed to fetch enough daily prices for {ticker.symbol}. Got {d1_prices and len(d1_prices)}')
    if not h1_prices or len(h1_prices) < H1_PRICES:
        raise Exception(f'Failed to fetch enough hourly prices for {ticker.symbol}. Got {h1_prices and len(h1_prices)}')
    
    d1_prices = torch.tensor(d1_prices[-D1_PRICES:], dtype=torch.float64)
    d1_volumes = torch.tensor(d1_volumes[-D1_PRICES:], dtype=torch.float64)
    h1_prices = torch.tensor(h1_prices[-H1_PRICES:], dtype=torch.float64)
    h1_volumes = torch.tensor(h1_volumes[-H1_PRICES:], dtype=torch.float64)

    #2. Normalize prices and volumes
    last_price = float(d1_prices[-1])
    d1_prices /= last_price
    h1_prices /= last_price

    shares = aggregate.get_shares_outstanding_at(ticker, end_time)
    d1_volumes /= shares
    h1_volumes /= shares
    market_cap = torch.tensor([last_price*shares], dtype=torch.float64)
    
    #3. Get the textual data
    market_summary = aggregate.get_market_summary(end_time)
    company_summary = aggregate.get_company_summary(ticker)
    titles = aggregate.get_company_news(ticker, d1_start_time, end_time)
    text_inputs = []
    for text in [market_summary, company_summary, titles]:
        inputs = tokenizer(text, return_tensors="pt", padding=True, truncation = True, max_length=512)
        with torch.no_grad():
            output = model(**inputs).pooler_output.flatten()
        text_inputs.append(output)
    
    return torch.cat([d1_prices, d1_volumes, h1_prices, h1_volumes, *text_inputs, market_cap], dim=0), last_price

def generate_example(
    ticker: nasdaq.NasdaqListedEntry,
    end_time: float
) -> torch.Tensor:
    """
    Generates a text example.
    The total number of prices will be prices_size, with prices_ration for 1d and (1-prices_ratio) for 1h prices.
    The last relevant market summary, the company summary, and the last 10 relevant news titles,
    will be passed as three text segments, to be processed by the language processor separately.
    The expected result is a number between -1 and 1, indicating the price action in the following day of trading (that is 8 hours).
        1 means that the price will rise 5 or more percent.
        0 means that the price will not rise.
        -1 means that the price will fall 5 or more percent.
        The inbetween is linearly distributed.
    Returns:
        (d1_prices, d1_volumes, h1_prices, h1_volumes, market_cap, market_summary, company_summary, titles, expect)
        Where expect is a value from 0 to 1.
    """
    data, last_price = generate_input(ticker, end_time)
    after_price, _ = aggregate.get_hourly_pricing(ticker, end_time+60, end_time+5*24*3600)
    if not after_price or len(after_price) < 7:
        raise Exception(f"Failed to fetch enough after prices for {ticker.symbol}")
    after_price = max(after_price[:8]) / last_price
    after_price -= 1
    results = [get_result(type, after_price) for type in result_types]
    results = torch.tensor(results, dtype=torch.float64)
    data = torch.concat([data, results], dim=0)

    logger.info(f'Generated example for {ticker.symbol} for end time {str(dateutils.unix_to_datetime(end_time))}')
    return data

def generate_current(
    ticker: nasdaq.NasdaqListedEntry
) -> torch.Tensor:
    return generate_input(ticker, time.time())[0]