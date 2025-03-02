
import logging
import torch
import matplotlib
import random
import time
from torch import Tensor
from pathlib import Path
from matplotlib import pyplot as plt
from ...utils.dateutils import TimingConfig, XNAS
from ...data import nasdaq, aggregate
from ..abstract import PriceEstimator, DataConfig, QUOTES, CLOSE_I, AFTER_KEY_PREFIX
from ..utils import check_tensors, PriceTarget, BatchFile
from .abstract import AbstractGenerator


logger = logging.getLogger(__name__)

class Generator(AbstractGenerator):
    def __init__(self,
        data_config: DataConfig,
        after_data_config: DataConfig,
        timing: TimingConfig,
        folder: Path
    ):
        self.data_config = data_config
        self.after_data_config = after_data_config
        self.timing = timing
        self.folder = folder

        self.tickers = [
            (
                it,
                XNAS.add_intervals(
                    aggregate.get_first_trade_time(it),
                    self.data_config.max_interval,
                    self.data_config.max_interval_count
                )
            )
            for it in aggregate.get_sorted_tickers()
        ]
        self.time_frame = (
            XNAS.add_intervals(
                self.data_config.min_interval.start_unix(), 
                self.data_config.min_interval, 
                self.data_config.min_Interval_count
            ),
            XNAS.add_intervals(
                time.time(), 
                self.after_data_config.max_interval,
                -self.after_data_config.max_interval_count
            )
        )

    def run(self):
        self._run_loop(
            folder = self.folder,
            timing = self.timing,
            tickers_fn=lambda unix_time: [it[0] for it in self.tickers if it[1] <= unix_time],
            time_frame=self.time_frame
        )

    def generate_example(
        self,
        ticker: nasdaq.NasdaqListedEntry,
        end_time: float,
        with_output: bool = True
    ) -> dict[str, Tensor]:
        #1. Get the prices
        data = {}
        for interval, count in self.data_config:
            start_time = XNAS.add_intervals(end_time, interval, -count)
            pricing = aggregate.get_interpolated_pricing(ticker, start_time, end_time, interval, return_quotes=QUOTES, max_fill_ratio=1/5)
            if len(pricing[0]) != count:
                raise Exception(f"Unexpected number of timestamps for start_time {start_time} end time {end_time} interval {interval} count {count}. Got {len(pricing[0])}.")
            data[interval.name] = torch.stack([torch.tensor(it, dtype=torch.float64) for it in pricing], dim=1)
        check_tensors(list(data.values()), allow_zeros=False)
        if not with_output: return data

        after_data = {}
        for interval, count in self.after_data_config:
            start_time = XNAS.add_intervals(end_time, interval, -count)
            pricing = aggregate.get_interpolated_pricing(ticker, start_time, end_time, interval, return_quotes=QUOTES, max_fill_ratio=1/5)
            if len(pricing[0]) != count:
                raise Exception(f"Unexpected number of timestamps for start_time {start_time} end time {end_time} interval {interval} count {count}. Got {len(pricing[0])}.")
            after_data[f"{AFTER_KEY_PREFIX}_{interval.name}"] = torch.stack([torch.tensor(it, dtype=torch.float64) for it in pricing], dim=1)
        check_tensors(list(after_data.values()), allow_zeros=False)
        return {**data, **after_data}

    def plot_statistics(
        self,
        estimator: PriceEstimator,
        targets: list[PriceTarget] = list(PriceTarget),
        title: str = ""
    ):
        #Bin distribution of after values
        temp = []
        files = [it.path for it in BatchFile.load(self.folder) if it.unix_time in self.timing]
        random.shuffle(files)
        for file in files[:20]:
            example = torch.load(file, weights_only=True)
            close = example[self.min_interval.name][:,-1:,CLOSE_I]
            data = (estimator.estimate_example(example) - close)/close
            temp.append(data)
        data = torch.concat(temp, dim=0)
        fig, axes = plt.subplots(1, 1+len(targets), figsize=(5,3+3*len(targets)))
        fig.suptitle(title)
        axes: list[matplotlib.axes.Axes] = axes
        
        axes[0].set_title(f'Raw')
        axes[0].set_xlabel('Percentage change')
        axes[0].hist(data*100, bins=range(-20,21,1), edgecolor='black')

        for i, target in enumerate(targets):
            axes[i].set_title(f"{target.name}")
            axes[i].set_xlabel('Expected output')
            axes[i].hist(target.get_price(data), bins=20, edgecolor='black')
        fig.tight_layout()
        plt.show()

