import logging
import time
import bisect
import matplotlib.lines
import torch
import matplotlib
import json
import random
from tqdm import tqdm
from pathlib import Path
from typing import Callable, Any, NamedTuple
from matplotlib import pyplot as plt
from ..data import nasdaq, aggregate
from ..utils import dateutils, plotutils
from ..utils.common import Interval, get_full_classname
from .utils import get_model_device, get_model_dtype
from .abstract import ExampleGenerator, AbstractModel

logger = logging.getLogger()

WIN = 'win'
REAL_WIN = 'real_win'
LAST_WIN = 'last_win'
REAL_LAST_WIN = 'real_last_win'

FOLDER = Path(__file__).parent / 'backtests'
if not FOLDER.exists(): FOLDER.mkdir()

class Result(NamedTuple):
    ticker: nasdaq.NasdaqListedEntry
    output: float
    data: dict = {}

class SelectionStrategy:
    results: list[Result]
    def __init__(self, top_count:int = 10):
        self.results = []
        self.top_count = top_count
    def insert(self, result: Result):
        bisect.insort_right(self.results, result, key=lambda it: -it.output)
    def get_selected(self) -> Result:
        return self.results[-1]
    def clear(self):
        self.results.clear()
    def to_json(self) -> str:
        return json.dumps(list({'ticker': it.ticker.to_line(), 'output': it.output, 'data': it.data} for it in self.results), indent=4)
    def to_config_dict(self) -> dict:
        return {
            'type': get_full_classname(self),
            'top_count': self.top_count
        }
class MarketCapSelector(SelectionStrategy):
    def __init__(self, top_count: int = 10):
        super().__init__(top_count)
    def insert(self, result):
        try:
            market_cap = aggregate.get_market_cap(result.ticker)
        except:
            market_cap = 0
        result.data['market_cap'] = market_cap
        return super().insert(result)
    def get_selected(self):
        return sorted(self.results[:self.top_count], key=lambda it: it.data['market_cap'])[-1]
    
class RandomSelector(SelectionStrategy):
    def __init__(self, top_count: int = 10):
        super().__init__(top_count)
    def get_selected(self) -> Result:
        random.choice(self.results[:self.top_count])

class FirstTradeTimeSelector(SelectionStrategy):
    def __init__(self, top_count: int = 10):
        super().__init__(top_count)
    def insert(self, result):
        result.data['first_trade_time'] = aggregate.get_first_trade_time(result.ticker)
        return super().insert(result)
    def get_selected(self):
        return sorted(self.results[:self.top_count], key=lambda it: it.data['first_trade_time'])[0]
    
class PriceEstimator:
    def __init__(self, interval: Interval, quote:str='high', last_count: int|None = None, min_count: int|None = None):
        self.interval = interval
        self.quote = quote[0].lower()
        self.last_count = last_count
        self.min_count = min_count
    def estimate_price(self, model: AbstractModel, ticker: nasdaq.NasdaqListedEntry, unix_time: float) -> float:
        end_time = dateutils.add_business_days_unix(unix_time, model.get_metadata().projection_period, tz=dateutils.ET)
        prices, = aggregate.get_pricing(ticker, unix_time, end_time, self.interval, return_quotes=[self.quote])
        if not prices:
            raise Exception(f"Got empty after prices for {ticker.symbol} at {unix_time}")
        if self.min_count and len(prices) < self.min_count:
            raise Exception(f"Not enough after prices. Got {len(prices)}, expecting at least {self.min_count}.")
        if self.last_count: prices = prices[-self.last_count:]
        if self.quote == 'h': return max(prices)
        if self.quote == 'l': return min(prices)
        if self.quote == 'o': return prices[0]
        if self.quote == 'c': return prices[-1]
        raise Exception(f"Unsupported quote {self.quote}")

    def to_dict(self) -> dict:
        return {
            'interval': self.interval.name,
            'quote': self.quote,
            'last_count': self.last_count,
            'min_count': self.min_count
        }

class Evaluator:
    def __init__(self, generator: ExampleGenerator, model: AbstractModel):
        self.generator = generator
        self.model = model
        self.device = get_model_device(model)
        self.dtype = get_model_dtype(model)

    def evaluate(
        self,
        tickers: list[nasdaq.NasdaqListedEntry]|None = None,
        unix_time: float|None = None,
        selector: SelectionStrategy = SelectionStrategy(),
        on_update: Callable[[SelectionStrategy], Any]|None = None,
        log: bool = True
    ) -> SelectionStrategy:
        """
        Evaluate model results for all tickers, on unix_time or live (if None).
        On update is invoked with an updated sorted list with each new result.
        """
        tickers = tickers or aggregate.get_sorted_tickers()
        logger.info(f"Evaluating {len(tickers)} tickers for {dateutils.unix_to_datetime(unix_time) if unix_time else 'live'}.")
        self.model.eval()
        with torch.no_grad():
            for ticker in tqdm(tickers, leave=True, desc=f"Evaluating for {dateutils.unix_to_datetime(unix_time)}"):
                try:
                    example = {key:value.to(dtype=self.dtype, device=self.device) for key,value in self.generator.generate_example(ticker, unix_time or time.time(), with_output=False).items()}
                    tensors = self.model.extract_tensors(example)
                    output = self.model(*tensors).squeeze().item()
                    selector.insert(Result(ticker, output))
                    if on_update: on_update(selector)
                    if log: logger.info(f"Evaluated {ticker.symbol} with output {output}.")
                except KeyboardInterrupt:
                    raise
                except:
                    if log: logger.error(f"Failed to evaluate {ticker.symbol}.", exc_info=True)
        return selector


    def backtest(
        self,
        unix_from: float,
        unix_to: float,
        hour: int = 14,
        commission: float = 0.0035,
        selector: SelectionStrategy = SelectionStrategy(),
        estimator: PriceEstimator = PriceEstimator(interval=Interval.H1, min_count=2),
        tickers: list[nasdaq.NasdaqListedEntry]|None = None
    ) -> float:
        """
        Returns total gain in percentages
        """
        logger.info(f"Backtesting from {dateutils.unix_to_datetime(unix_from)} to {dateutils.unix_to_datetime(unix_to)}")
        unix_time = unix_from
        tickers = tickers or aggregate.get_sorted_tickers()
        history = {
            WIN: [1],
            REAL_WIN: [1],
            LAST_WIN: [1],
            REAL_LAST_WIN: [1]
        }
        plt.ion()
        fig1, ax1 = plt.subplots(1,1)
        fig2, ax2 = plt.subplots(1,1)
        ax1.set_title('Daily gain')
        ax2.set_title('Cumulative gain')
        ax1.set_xlabel('Days')
        ax2.set_xlabel('Days')
        ax1.scatt
        mock = [1,2]
        lines: dict[str, matplotlib.lines.Line2D] = {
            LAST_WIN: ax1.plot(mock, mock, color = 'blue', label = LAST_WIN, marker='o', markersize=2, linestyle='')[0],
            REAL_LAST_WIN: ax1.plot(mock, mock, color='red', label = REAL_LAST_WIN, marker='o', markersize=2, linestyle='')[0],
            WIN: ax2.plot(mock, mock, color = 'blue', label = WIN)[0],
            REAL_WIN: ax2.plot(mock, mock, color = 'red', label = REAL_WIN)[0],
        }
        ax1.legend()
        ax2.legend()
        plotutils.refresh_interactive_figures(fig1, fig2)
        
        while True:
            unix_time = dateutils.get_next_working_time(unix_time, hour=hour)
            if unix_time >= unix_to:
                break
            try:
                #Take the largest mcap of the top 10
                selector.clear()
                self.evaluate(tickers, unix_time=unix_time, log=False, selector=selector)[:10]
                
                result = selector.get_selected()
                
                last_price = aggregate.get_pricing(result.ticker, unix_from=unix_time-24*3600, unix_to=unix_time, interval=Interval.H1, return_quotes=['close'])[0][-1]
                sell_price = estimator.estimate_price(self.model, result.ticker, unix_time)
                history[LAST_WIN].append(sell_price/last_price)
                history[REAL_LAST_WIN].append((sell_price-commission*sell_price-commission*last_price)/last_price)
                history[WIN].append(history[WIN][-1]*history[LAST_WIN][-1])
                history[REAL_WIN].append(history[REAL_WIN][-1]*history[REAL_LAST_WIN][-1])
                logger.info(f"Buying {result.ticker.symbol} at {dateutils.unix_to_datetime(unix_time)}.")
                logger.info(f"Output: {result.output}. Data: {result.data}.")
                logger.info(f"WIN: {history[LAST_WIN][-1]}. REAL WIN {history[REAL_LAST_WIN][-1]}")

                lines[LAST_WIN].set_data(range(len(history[LAST_WIN])), history[LAST_WIN])
                lines[REAL_LAST_WIN].set_data(range(len(history[REAL_LAST_WIN])), history[REAL_LAST_WIN])
                lines[WIN].set_data(range(len(history[WIN])), history[WIN])
                lines[REAL_WIN].set_data(range(len(history[REAL_WIN])), history[REAL_WIN])
                plotutils.refresh_interactive_figures(fig1, fig2)
            except KeyboardInterrupt:
                raise
            except:
                logger.error(f"Failed to evaluate at {dateutils.unix_to_datetime(unix_time)}.", exc_info=True)
        plt.ioff()
        model_name = type(self.model).__name__
        tosave = {
            'history': history,
            'unix_from': unix_from,
            'unix_to': unix_to,
            'model': model_name,
            'hour': hour,
            'selector': selector.to_config_dict(),
            'estimator': estimator.to_dict()
        }
        path = FOLDER / f"backtest_{model_name}_hour{hour}_t{int(time.time())}.json"
        path.write_text(json.dumps(tosave))
        plt.show(block = True)
        return history



