import logging
import time
import bisect
from torch.nn import Module
from typing import Callable, Any
from matplotlib import pyplot as plt
from ..data import nasdaq, aggregate
from ..utils import dateutils
from ..utils.common import Interval
from .abstract import ExampleGenerator, TensorExtractor

logger = logging.getLogger()

WIN = 'win'
REAL_WIN = 'real_win'
LAST_WIN = 'last_win'
REAL_LAST_WIN = 'real_last_win'

class Evaluator:

    def __init__(self, generator: ExampleGenerator, extractor: TensorExtractor, model: Module):
        self.generator = generator
        self.extractor = extractor
        self.model = model

    def evaluate(
        self,
        tickers: list[nasdaq.NasdaqListedEntry]|None = None,
        unix_time: float|None = None,
        on_update: Callable[[list[dict]], Any]|None = None
    ) -> list[nasdaq.NasdaqListedEntry]:
        """
        Evaluate model results for all tickers, on unix_time or live (if None).
        On update is invoked with an updated sorted list with each new result.
        """
        tickers = tickers or aggregate.get_sorted_tickers()
        logger.info(f"Evaluating {len(tickers)} tickers for {dateutils.unix_to_datetime(unix_time) if unix_time else 'live'}.")
        results = []
        for ticker in tickers:
            try:
                example = self.generator.generate_example(ticker, unix_time or time.time(), with_output=False)
                tensors = self.extractor.extract_tensors(example)
                output = self.model(*tensors).squeeze().item()
                try:
                    market_cap = aggregate.get_market_cap(ticker)
                except:
                    market_cap = 0
                result = {
                    'ticker': ticker.to_line(),
                    'market_cap': market_cap,
                    'output': output
                }
                bisect.insort_right(results, result, key=lambda it: it['output'])
                if on_update: on_update(results)
                logger.info(f"Evaluated {ticker.symbol} with output {output}.")
            except:
                logger.error(f"Failed to evaluate {ticker.symbol}.", exc_info=True)
        return results


    def backtest(
        self,
        unix_from: float,
        unix_to: float,
        hour: int = 14,
        commission: float = 0,
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
        ax1.set_title('Cumulative gain')
        ax2.set_title('Daily gain')
        ax1.set_xlabel('Days') and ax2.set_xlabel('Days')
        lines = {
            LAST_WIN: ax1.scatter([], [], s=0.1, color = 'blue', label = LAST_WIN),
            REAL_LAST_WIN: ax1.scatter([], [], s=0.1, color='red', label = REAL_LAST_WIN),
            WIN: ax1.plot([], [], color = 'blue', label = WIN)[0],
            REAL_WIN: ax1.plot([], [], color = 'red', label = REAL_WIN)[0],
        }
        ax1.legend() and ax2.legend()
        
        while True:
            unix_time = dateutils.get_next_working_time(unix_time, hour=hour)
            if unix_time >= unix_to:
                break
            try:
                #Take the largest mcap of the top 10
                results = self.evaluate(tickers, unix_time=unix_time)[:10]
                result = sorted(results, key=lambda it: it['market_cap'])[-1]
                
                ticker = nasdaq.NasdaqListedEntry.from_line(result['ticker'])
                market_cap = result['market_cap']
                output = result['output']
                
                last_price = aggregate.get_pricing(ticker, unix_from=unix_time-24*3600, unix_to=unix_time, interval=Interval.H1, return_quotes='close')[0][-1]
                max_price = max(aggregate.get_pricing(ticker, unix_from=unix_time+1, unix_to=dateutils.add_business_days_unix(unix_time, 1, tz=dateutils.ET), interval=Interval.H1, return_quotes=['high'])[0])
                win = max_price/last_price
                real_win = (max_price-commission*max_price-commission*last_price)/last_price
                total_win *= win
                total_real_win *= real_win
                history[LAST_WIN].append(max_price/last_price)
                history[REAL_LAST_WIN].append((max_price-commission*max_price-commission*last_price)/last_price)
                history[WIN].append(history[WIN][-1]*history[LAST_WIN][-1])
                history[REAL_LAST_WIN].append(history[REAL_WIN][-1]*history[REAL_LAST_WIN][-1])
                logger.info(f"Buying {ticker.symbol} at {dateutils.unix_to_datetime(unix_time)}.")
                logger.info(f"Output: {output}. Market cap: {market_cap}.")
                logger.info(f"WIN: {win}. REAL WIN {real_win}")

                lines[LAST_WIN].set_offsets([[i, history[LAST_WIN][i]] for i in range(len(history[LAST_WIN]))])
                lines[REAL_LAST_WIN].set_offsets([[i, history[REAL_LAST_WIN][i]] for i in range(len(history[REAL_LAST_WIN]))])
                lines[WIN].set_data(range(len(history[WIN])), history[WIN])
                lines[REAL_WIN].set_data(range(len(history[REAL_WIN])), history[REAL_WIN])
                ax1.relim()
                ax2.relim()
                ax1.autoscale_view()
                ax2.autoscale_view()
                fig1.canvas.draw()
                fig2.canvas.draw()
                fig1.canvas.flush_events()
                fig2.canvas.flush_events()
            except:
                logger.error(f"Failed to evaluate at {dateutils.unix_to_datetime(unix_time)}.", exc_info=True)
        plt.ioff()
        plt.show(block = True)
        return history



