#1
from __future__ import annotations
import logging
import os
import time
import bisect
import torch
import matplotlib
import random
from tqdm import tqdm
from pathlib import Path
from typing import Callable, Any, Sequence, override
import matplotlib.lines
from matplotlib import pyplot as plt
from matplotlib.gridspec import GridSpec

from base import dates
from base import text
from base.types import ClassDict
from base.serialization import serializable, Serializable, serializer
from base import plotutils
from trading.core.timing_config import TimingConfig
from trading.core import Interval
from trading.core.securities import Exchange, Security, SecurityType
from trading.models.base import ModelManager
from trading.providers.aggregate import AggregateProvider
from trading.models.base.model_config import PriceEstimator, BaseModelConfig
from trading.models.base.abstract_model import AbstractModel
from trading.models.generators.abstract_generator import AbstractGenerator
logger = logging.getLogger()

FOLDER = Path(__file__).parent / 'backtests'
if not FOLDER.exists(): FOLDER.mkdir()

@serializable()
class Result(Serializable):
    def __init__(self, security: Security, output: float):
        self.security = security
        self.output = output
        self.data = {}

@serializable()
class Selector(Serializable):
    results: list[Result]
    def __init__(self, top_count:int = 10):
        self.results = []
        self.top_count = top_count
    def insert(self, result: Result):
        bisect.insort_right(self.results, result, key=lambda it: -it.output)
    def get_selected(self) -> Sequence[Result]:
        """
        Returns results sorted from best to worst.
        """
        return self.results
    def clear(self):
        self.results.clear()

class MarketCapSelector(Selector):
    def __init__(self, top_count: int = 10, select_at: float = 1):
        super().__init__(top_count)
        self.select_at = select_at
    def insert(self, result):
        try:
            market_cap = AggregateProvider.instance.get_market_cap(result.security)
        except:
            market_cap = 0
        result.data['market_cap'] = market_cap
        return super().insert(result)
    @override
    def get_selected(self) -> Sequence[Result]:
        selection = sorted(self.results[:self.top_count], key=lambda it: -it.data['market_cap'])
        return selection[round(self.select_at*(len(selection)-1)):]

class RandomSelector(Selector):
    def __init__(self, top_count: int = 10):
        super().__init__(top_count)
    @override
    def get_selected(self) -> list[Result]:
        selection = self.results[:self.top_count]
        random.shuffle(selection)
        return selection

class FirstTradeTimeSelector(Selector):
    KEY = 'first_trade_time'
    def __init__(self, top_count: int = 10):
        super().__init__(top_count)
    def insert(self, result):
        result.data[FirstTradeTimeSelector.KEY] = AggregateProvider.instance.get_first_trade_time(result.security)
        return super().insert(result)
    def get_selected(self):
        return sorted(self.results[:self.top_count], key=lambda it: it.data[FirstTradeTimeSelector.KEY])

@serializable()
class BacktestFrame(Serializable, ClassDict[float]):
    def __init__(self, win: float, total_win: float, real_win: float, total_real_win: float):
        self.win = win
        self.total_win = total_win
        self.real_win = real_win
        self.total_real_win = total_real_win
    
    @staticmethod
    def create(buy_price: float, sell_price: float, commission: float, prev: BacktestFrame|None) -> BacktestFrame:
        win = sell_price/buy_price
        real_win = (sell_price-commission*sell_price-commission*buy_price)/buy_price
        total_win = prev.total_win*win if prev else win
        total_real_win = prev.total_real_win*real_win if prev else real_win
        return BacktestFrame(win, total_win, real_win, total_real_win)

@serializable()
class BacktestResult(Serializable):
    def __init__(
        self,
        history: list[BacktestFrame],
        unix_from: float,
        unix_to: float,
        selector: Selector,
        estimator: PriceEstimator,
        commission: float,
        model: str
    ):
        self.history = history
        self.unix_from = unix_from
        self.unix_to = unix_to
        self.selector = selector
        self.estimator = estimator
        self.commission = commission
        self.model = model
        self.unix_time = time.time()
    
    def __str__(self) -> str:
        return f"""BacktestResult ({dates.unix_to_datetime(self.unix_from,tz=dates.CET)} -> {dates.unix_to_datetime(self.unix_to,tz=dates.CET)}).
    Selector = {self.selector}
    Estimator = {self.estimator}
    Model = '{self.model}'
    Time = {dates.unix_to_datetime(self.unix_time, tz=dates.CET)}
"""

class Evaluator:
    def __init__(self, manager: ModelManager, generator: AbstractGenerator):
        self.generator = generator
        self.manager = manager

    def evaluate(self, security: Security, unix_time: float|None = None) -> float:
        self.manager.model.eval()
        with torch.no_grad():
            example = {key:value.to(dtype=self.manager.dtype, device=self.manager.device) for key,value in self.generator.generate_example(security, unix_time or time.time(), with_output=False).items()}
            tensors = self.manager.model.extract_tensors(example, with_output=False)
            return self.manager.model(tensors).squeeze().item()

    def select(
        self,
        securities: list[Security],
        unix_time: float|None = None,
        selector: Selector = Selector(),
        on_update: Callable[[Selector], None]|None = None,
        log: bool = True
    ) -> None:
        """
        Evaluate model results for all securities, on unix_time or live (if None).
        On update is invoked with each new result.
        """
        logger.info(f"Evaluating {len(securities)} securities for {dates.unix_to_datetime(unix_time, tz=dates.CET) if unix_time else 'live'}.")
        if isinstance(selector, RandomSelector) and selector.top_count >= len(securities):
            for security in securities: selector.insert(Result(security, 0))
            return
        for security in tqdm(securities, leave=True, desc=f"Evaluating for {dates.unix_to_datetime(unix_time, tz=dates.CET) if unix_time else 'now'}"):
            try:
                output = self.evaluate(security, unix_time)
                selector.insert(Result(security, output))
                if on_update: on_update(selector)
                if log: logger.info(f"Evaluated {security.symbol} with output {output}.")
            except KeyboardInterrupt:
                raise
            except:
                if log: logger.error(f"Failed to evaluate {security.symbol}.", exc_info=True)
        return

    def backtest(
        self,
        unix_from: float,
        unix_to: float,
        exchange: Exchange,
        timing: TimingConfig,
        estimator: PriceEstimator,
        selector: Selector = Selector(),
        commission: float = 0.0035
    ) -> BacktestResult:
        """
        Returns total gain in percentages
        """
        securities = [it for it in exchange.get_securities() if it.type == SecurityType.STOCK]
        logger.info(f"Backtesting on {exchange.mic} from {exchange.calendar.unix_to_datetime(unix_from)} to {exchange.calendar.unix_to_datetime(unix_to)}")
        unix_time = unix_from
        history: list[BacktestFrame] = []
        plt.ion()
        fig1, ax1 = plt.subplots(1,1)
        fig2, ax2 = plt.subplots(1,1)
        ax1.set_title('Daily gain')
        ax2.set_title('Cumulative gain')
        ax1.set_xlabel('Days')
        ax2.set_xlabel('Days')
        mock = [1,2]
        lines: dict[str, matplotlib.lines.Line2D] = {
            'win': ax1.plot(mock, mock, color = 'blue', label = 'Win', marker='o', markersize=2, linestyle='')[0],
            'real_win': ax1.plot(mock, mock, color='red', label = 'Real Win', marker='o', markersize=2, linestyle='')[0],
            'total_win': ax2.plot(mock, mock, color = 'blue', label = 'Total Win')[0],
            'real_win': ax2.plot(mock, mock, color = 'red', label = 'Real Win')[0],
        }
        ax1.legend()
        ax2.legend()
        plotutils.refresh_interactive_figures(fig1, fig2)
        
        unix_time = timing.next(unix_time, Interval.M5, exchange)
        while unix_time < unix_to:
            try:
                selector.clear()
                self.select(securities, unix_time=unix_time, log=False, selector=selector)
                
                results = selector.get_selected()
                result: Result|None = None
                sell_price: float = 1.0
                buy_price: float = 1.0
                for it in results:
                    try:
                        sell_price = estimator.estimate(it.security, unix_time)
                        buy_price = AggregateProvider.instance.get_pricing(unix_time-24*3600, unix_time, it.security, Interval.M5)[-1].c
                        result = it
                        break
                    except:
                        logger.warning(f"Failed to estimate sell price for {it.security.symbol}", exc_info=True)
                if not result: raise Exception(f"Failed to estimate sell price for all selected entries.")

                history.append(BacktestFrame.create(buy_price, sell_price, commission, history[-1] if history else None))
                logger.info(f"Buying {result.security.symbol} at {dates.unix_to_datetime(unix_time,tz=dates.CET)}.")
                logger.info(f"Output: {result.output}. Data: {result.data}.")
                logger.info(f"REAL WIN: {history[-1].win}")

                x = list(range(len(history)))
                for key in lines.keys():
                    lines[key].set_data(x, [it[key] for it in history])
                plotutils.refresh_interactive_figures(fig1, fig2)
            except KeyboardInterrupt:
                raise
            except:
                logger.error(f"Failed to evaluate at {dates.unix_to_datetime(unix_time,tz=dates.CET)}.", exc_info=True)
            unix_time = timing.next(unix_time, Interval.M5, exchange)
            
        plt.ioff()
        selector.clear()
        model = f"{self.manager.model.get_name()}\n{text.tab(serializer.serialize(self.manager.model.config, typed=False, indent=2))}"
        backtest_result = BacktestResult(history, unix_from, unix_to, selector, estimator, commission, model)
        path = self.manager.backtests / f"backtest_t{int(time.time())}.json"
        path.write_text(serializer.serialize(backtest_result))
        plt.show(block = True)
        return backtest_result

    @staticmethod
    def show_backtest_file(file: Path, block: bool = True):
        data = serializer.deserialize(file.read_text(), BacktestResult)
        Evaluator.show_backtest(data, block=block)

    def show_backtests(self, unix_from: float, unix_to: float):
        backtests = [serializer.deserialize((self.manager.backtests/file).read_text(), BacktestResult) for file in os.listdir(self.manager.backtests)]
        backtests = [it for it in backtests if it.unix_time > unix_from and it.unix_time <= unix_to]
        backtests = sorted(backtests, key=lambda it: it.unix_time)
        for backtest in backtests:
            Evaluator.show_backtest(backtest)

    @staticmethod
    def show_backtest(data: BacktestResult, block: bool = True):
        history = data.history
        fig = plt.figure(figsize=(6,4))
        gs = GridSpec(4, 2, figure=fig)
        ax_title = fig.add_subplot(gs[0,:])
        ax_left = fig.add_subplot(gs[1:,0])
        ax_right = fig.add_subplot(gs[1:,1])
        ax_title.text(0.5, 1, str(data), ha='center', va='top', fontsize=10)
        ax_title.grid(False)
        ax_title.axis('off')
        
        ax_left.set_title('Daily gain')
        ax_right.set_title('Cumulative gain')
        ax_left.set_xlabel('Days')
        ax_right.set_xlabel('Days')

        x = range(len(history))
        ax_left.plot(x, [it.win for it in history], color = 'blue', label = 'Win', marker='o', markersize=2, linestyle='')
        ax_left.plot(x, [it.real_win for it in history], color='red', label = 'Real Win', marker='o', markersize=2, linestyle='')
        ax_right.plot(x, [it.total_win for it in history], color = 'blue', label = 'Total Win')
        ax_right.plot(x, [it.total_real_win for it in history], color = 'red', label = 'Total Real Win')
        ax_left.legend()
        ax_right.legend()

        plt.show(block=block)
        return
