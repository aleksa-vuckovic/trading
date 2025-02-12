import logging
import torch
from torch import Tensor
from pathlib import Path
from transformers import BertTokenizer, BertModel
from ...utils import dateutils
from ...utils.common import Interval
from ...data import aggregate, nasdaq
from ..utils import get_normalized_by_largest, check_tensor
from ..abstract import ExampleGenerator

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

DATA = 'data'

FOLDER = Path(__file__).parent / 'examples'
class Generator(ExampleGenerator):
    def run(self):
        return self.run_loop(
            folder = FOLDER,
            hour = 16
        )

    def generate_example(
        self,
        ticker: nasdaq.NasdaqListedEntry,
        end_time: float,
        with_output: bool = True
    ) -> dict[str, Tensor]:
        #1. Get the prices
        d1_start_time = (end_time-(D1_PRICES/5*7*1.2+5)*24*3600)
        h1_start_time = (end_time-(H1_PRICES/6/5*7*1.5+5)*24*3600)

        d1_prices, d1_volumes = aggregate.get_pricing(ticker, d1_start_time, end_time, Interval.D1, return_quotes=['close', 'volume'])
        if len(d1_prices) < D1_PRICES:
            raise Exception(f'Failed to fetch enough daily prices for {ticker.symbol}. Got {len(d1_prices)}')
        h1_prices, h1_volumes = aggregate.get_pricing(ticker, h1_start_time, end_time, Interval.H1, return_quotes=['close', 'volume'])
        if len(h1_prices) < H1_PRICES:
            raise Exception(f'Failed to fetch enough hourly prices for {ticker.symbol}. Got {len(h1_prices)}')
        
        last_price = float(h1_prices[-1])
        d1_prices = torch.tensor(d1_prices[-D1_PRICES:], dtype=torch.float64)
        d1_volumes = torch.tensor(d1_volumes[-D1_PRICES:], dtype=torch.float64)
        h1_prices = torch.tensor(h1_prices[-H1_PRICES:], dtype=torch.float64)
        h1_volumes = torch.tensor(h1_volumes[-H1_PRICES:], dtype=torch.float64)
        market_cap = torch.tensor([last_price*aggregate.get_shares_outstanding_at(ticker, end_time)], dtype=torch.float64)
        
        #2. Normalize prices and volumes
        d1_prices = get_normalized_by_largest(d1_prices)
        d1_volumes = get_normalized_by_largest(d1_volumes)
        h1_prices = get_normalized_by_largest(h1_prices)
        h1_volumes = get_normalized_by_largest(h1_volumes)

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
        
        data = torch.cat([d1_prices, d1_volumes, h1_prices, h1_volumes, *text_inputs, market_cap], dim=0)
        if with_output:
            h_after_prices, = aggregate.get_pricing(ticker, end_time, dateutils.add_business_days_unix(end_time, 3, tz=dateutils.ET), Interval.H1, return_quotes=['close'])
            d_after_prices, = aggregate.get_pricing(ticker, end_time, dateutils.add_business_days_unix(end_time, 5, tz=dateutils.ET), Interval.D1, return_quotes=['close'])
            if len(h_after_prices) < 7:
                raise Exception(f"Failed to fetch enough hourly after prices for {ticker.symbol}. Got {len(h_after_prices)}.")
            if len(d_after_prices) < 3:
                raise Exception(f"Failed to fetch enough daily after prices for {ticker.symbol}. Got {len(d_after_prices)}.")
            after_prices = torch.tensor([max(h_after_prices[:8]), max(h_after_prices[:15]), max(d_after_prices[:5])], dtype=torch.float64) / last_price - 1
            data = torch.cat([data, after_prices], dim=0)
        
        check_tensor(data)
        return {'data': data}
    