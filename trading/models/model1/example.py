import time
import logging
import torch
from transformers import BertTokenizer, BertModel
from ...utils import dateutils, common
from ...utils.common import Interval
from ...data import aggregate, nasdaq
from ..utils import normalize_in_place

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
D1_TARGET_I = MARKET_CAP_I + 1
D2_TARGET_I = D1_TARGET_I + 1
D7_TARGET_I = D2_TARGET_I + 1

def generate_input(
    ticker: nasdaq.NasdaqListedEntry,
    end_time: float
) -> tuple[torch.Tensor, float]:
    #1. Get the prices
    d1_start_time = (end_time-(D1_PRICES/5*7*1.2+5)*24*3600)
    h1_start_time = (end_time-(H1_PRICES/6/5*7*1.5+5)*24*3600)

    d1_prices, d1_volumes = aggregate.get_pricing(ticker, d1_start_time, end_time - common.get_delay_for_interval(Interval.D1), Interval.D1, return_quotes=['close', 'volume'])
    if len(d1_prices) < D1_PRICES:
        raise Exception(f'Failed to fetch enough daily prices for {ticker.symbol}. Got {len(d1_prices)}')
    h1_prices, h1_volumes = aggregate.get_pricing(ticker, h1_start_time, end_time - common.get_delay_for_interval(Interval.H1), Interval.H1, return_quotes=['close', 'volume'])
    if len(h1_prices) < H1_PRICES:
        raise Exception(f'Failed to fetch enough hourly prices for {ticker.symbol}. Got {len(h1_prices)}')
    
    last_price = float(h1_prices[-1])
    d1_prices = torch.tensor(d1_prices[-D1_PRICES:], dtype=torch.float64)
    d1_volumes = torch.tensor(d1_volumes[-D1_PRICES:], dtype=torch.float64)
    h1_prices = torch.tensor(h1_prices[-H1_PRICES:], dtype=torch.float64)
    h1_volumes = torch.tensor(h1_volumes[-H1_PRICES:], dtype=torch.float64)
    market_cap = torch.tensor([last_price*aggregate.get_shares_outstanding_at(ticker, end_time)], dtype=torch.float64)
    
    #2. Normalize prices and volumes
    normalize_in_place(d1_prices)
    normalize_in_place(d1_volumes)
    normalize_in_place(h1_prices)
    normalize_in_place(h1_volumes)

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
    
    result = torch.cat([d1_prices, d1_volumes, h1_prices, h1_volumes, *text_inputs, market_cap], dim=0)
    bad_entries = torch.logical_or(result.isnan(), result.isinf()).sum().item()
    if bad_entries > 0:
        raise Exception(f"Generated example has {bad_entries} nans or infs.")
    return result, last_price

def generate_example(
    ticker: nasdaq.NasdaqListedEntry,
    end_time: float
) -> torch.Tensor:
    """
    Generates a text example.
    The total number of prices will be prices_size, with prices_ration for 1d and (1-prices_ratio) for 1h prices.
    The last relevant market summary, the company summary, and the last 10 relevant news titles,
    will be passed as three text segments, to be processed by the language processor separately.
    Returns:
        (d1_prices, d1_volumes, h1_prices, h1_volumes, market_cap, market_summary, company_summary, titles, expect)
        Where expect is a value from 0 to 1.
    """
    data, last_price = generate_input(ticker, end_time)
    h_after_prices, = aggregate.get_pricing(ticker, end_time, dateutils.add_business_days_unix(end_time, 3, tz=dateutils.ET), Interval.H1, return_quotes=['close'])
    d_after_prices, = aggregate.get_pricing(ticker, end_time, dateutils.add_business_days_unix(end_time, 5, tz=dateutils.ET), Interval.D1, return_quotes=['close'])
    if len(h_after_prices) < 7:
        raise Exception(f"Failed to fetch enough hourly after prices for {ticker.symbol}. Got {len(h_after_prices)}.")
    if len(d_after_prices) < 3:
        raise Exception(f"Failed to fetch enough daily after prices for {ticker.symbol}. Got {len(d_after_prices)}.")
    after_prices = torch.tensor([max(h_after_prices[:8]), max(h_after_prices[:15]), max(d_after_prices[:5])], dtype=torch.float64) / last_price - 1
    data = torch.concat([data, after_prices], dim=0)
    logger.info(f'Generated example for {ticker.symbol} for end time {str(dateutils.unix_to_datetime(end_time))}')
    return data

def generate_current(
    ticker: nasdaq.NasdaqListedEntry
) -> torch.Tensor:
    return generate_input(ticker, time.time())[0]