
import logging
import torch
import random
import time
from typing import Sequence, override
from matplotlib.axes import Axes
from matplotlib import pyplot as plt
from torch import Tensor
from pathlib import Path

from trading.core import Interval
from trading.core.securities import Security
from trading.core.work_calendar import TimingConfig
from trading.providers.aggregate import AggregateProvider
from trading.models.base.model_config import Aggregation, Quote, AFTER, PriceEstimator, DataConfig, PriceTarget
from trading.models.base.tensors import check_tensors
from trading.models.base.batches import BatchFile
from trading.models.generators.abstract_generator import AbstractGenerator

logger = logging.getLogger(__name__)

class Generator(AbstractGenerator):
    def __init__(
        self,
        securities: Sequence[Security],
        data_config: DataConfig,
        after_data_config: DataConfig,
        timing: TimingConfig,
        folder: Path
    ):
        self.data_config = data_config
        self.after_data_config = after_data_config
        self.timing = timing
        self.folder = folder

        # securities with the first unix time that an example can be generated for them
        self.securities = [
            (
                it,
                max(
                    it.exchange.calendar.add_intervals(
                        AggregateProvider.instance.get_first_trade_time(it),
                        interval,
                        self.data_config.counts[interval]
                    )
                    for interval in self.data_config.intervals
                )
            )
            for it in securities
        ]

        exchanges = set(it.exchange for it in securities)
        
        # the time frame for the entire generator loop depends mainly on the providers' data time frames
        self.time_frame = (
            max(
                exchange.calendar.add_intervals(
                    AggregateProvider.instance.get_interval_start(interval), 
                    interval, 
                    self.data_config.counts[interval]
                ) for interval in self.data_config.intervals for exchange in exchanges
            ),
            min(
                exchange.calendar.add_intervals(
                    time.time(),
                    interval,
                    -self.after_data_config.counts[interval]
                )
                for interval in self.data_config.intervals for exchange in exchanges
            )
        )

    def run(self):
        self._run_loop(
            folder = self.folder,
            timing = self.timing,
            securities_fn=lambda unix_time: [it[0] for it in self.securities if it[1] <= unix_time],
            time_frame=self.time_frame
        )

    @override
    def generate_example(
        self,
        security: Security,
        end_time: float,
        with_output: bool = True
    ) -> dict[str, Tensor]:
        #1. Get the prices
        data = {}
        for interval, count in self.data_config:
            start_time = security.exchange.calendar.add_intervals(end_time, interval, -count)
            pricing = AggregateProvider.instance.get_pricing(start_time, end_time, security, interval, interpolate=True, max_fill_ratio=1/5)
            if len(pricing) != count: 
                raise Exception(f"Unexpected number of timestamps for start_time {start_time} end time {end_time} interval {interval} count {count}. Got {len(pricing)}.")
            data[interval.name] = [[it[quote.name] for quote in Quote] for it in pricing]
        check_tensors(list(data.values()), allow_zeros=False)
        if not with_output: return data

        after_data = {}
        for interval, count in self.after_data_config:
            start_time = security.exchange.calendar.add_intervals(end_time, interval, -count)
            pricing = AggregateProvider.instance.get_pricing(start_time, end_time, security, interval, interpolate=True, max_fill_ratio=1/5)
            if len(pricing) != count:
                raise Exception(f"Unexpected number of timestamps for start_time {start_time} end time {end_time} interval {interval} count {count}. Got {len(pricing)}.")
            
            after_data[f"{AFTER}_{interval.name}"] = torch.tensor([[it[quote.name] for quote in Quote] for it in pricing], dtype=torch.float64)
        check_tensors(list(after_data.values()), allow_zeros=False)
        return {**data, **after_data}

    @override
    def plot_statistics(
        self,
        estimator: PriceEstimator = PriceEstimator(Quote.C, Interval.H1, slice(0,7), Aggregation.LAST),
        targets: list[PriceTarget] = list(PriceTarget),
        title: str = "",
        **kwargs
    ):
        #Bin distribution of after values
        temp: list[Tensor] = []
        files = [it.path for it in BatchFile.load(self.folder) if it.unix_time in self.timing]
        random.shuffle(files)
        for file in files[:20]:
            example: dict[str, Tensor] = torch.load(file, weights_only=True)
            close = example[self.data_config.min_interval.name][:,-1:,Quote.C.value]
            data = (estimator.estimate_example(example) - close)/close
            temp.append(data)
        data = torch.concat(temp, dim=0)
        fig, axes = plt.subplots(1, 1+len(targets), figsize=(5,3+3*len(targets)))
        fig.suptitle(title)
        axes: list[Axes] = axes
        
        axes[0].set_title(f'Raw')
        axes[0].set_xlabel('Percentage change')
        axes[0].hist(data*100, bins=range(-20,21,1), edgecolor='black')

        for i, target in enumerate(targets):
            axes[i].set_title(f"{target.name}")
            axes[i].set_xlabel('Expected output')
            axes[i].hist(target.get_price(data), bins=20, edgecolor='black')
        fig.tight_layout()
        plt.show()

