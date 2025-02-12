import logging
import time
import bisect
from torch.nn import Module
from typing import Callable
from ..data import nasdaq, aggregate
from ..utils import dateutils
from .abstract import ExampleGenerator, TensorExtractor

logger = logging.getLogger()

class Evaluator:

    def __init__(self, generator: ExampleGenerator, extractor: TensorExtractor, model: Module):
        self.generator = generator
        self.extractor = extractor
        self.model = model

    def evaluate(
        self,
        tickers: list[nasdaq.NasdaqListedEntry]|None = None,
        unix_time: float|None = None,
        on_update: Callable[[list[dict]]]|None = None
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
                    'symbol': ticker.symbol,
                    'name': ticker.name,
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
        tickers: list[nasdaq.NasdaqListedEntry]|None = None
    ) -> float:
        """
        Returns total gain in percentages
        """
        while True:
            pass