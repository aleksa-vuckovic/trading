
import logging
import torch
import matplotlib
import random
import time
from torch import Tensor
from pathlib import Path
from matplotlib import pyplot as plt
from ...utils import dateutils
from ...utils.dateutils import TimingConfig
from ...utils.common import Interval
from ...data import nasdaq, aggregate
from ..abstract import PriceEstimator, QUOTES, CLOSE_I, OUTPUT_KEY_PREFIX
from ..utils import check_tensors, PriceTarget, BatchFile
from .abstract import AbstractGenerator


logger = logging.getLogger(__name__)

class Generator(AbstractGenerator):
    def __init__(self,
        data_points: dict[Interval, int],
        after_data_points: dict[Interval, int],
        timing: TimingConfig,
        folder: Path
    ):
        self.data_points = data_points
        self.after_data_points = after_data_points
        self.timing = timing
        self.folder = folder

        self.max_interval = sorted(data_points.keys())[-1]
        self.min_interval = sorted(data_points.keys())[0]
        self.after_max_interval = sorted(after_data_points.keys())[-1]
        self.after_min_interval = sorted(after_data_points.keys())[0]

        self.tickers = [
            (
                it,
                dateutils.add_intervals_unix(
                    aggregate.get_first_trade_time(it),
                    self.max_interval,
                    data_points[self.max_interval]
                )
            )
            for it in aggregate.get_sorted_tickers()
        ]
        self.time_frame = (
            dateutils.add_intervals_unix(self.min_interval.start_unix(), self.min_interval, data_points[self.min_interval]),
            dateutils.add_intervals_unix(time.time(), self.after_max_interval, -after_data_points[self.after_max_interval])
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
        for interval, count in self.data_points:
            start_time = dateutils.add_intervals_unix(end_time, interval, -count)
            pricing = aggregate.get_interpolated_pricing(ticker, start_time, end_time, interval, return_quotes=QUOTES, max_fill_ratio=0.2)
            if len(pricing[0]) != count:
                raise Exception(f"Unexpected number of timestamps for start_time {start_time} end time {end_time} interval {interval} count {count}. Got {len(data[interval])}.")
            data[interval.name] = torch.stack([torch.tensor(it, dtype=torch.float64) for it in data], dim=1)
        check_tensors(list(data.values()), allow_zeros=False)
        if not with_output: return data

        after_data = {}
        for interval, count in self.after_data_points:
            start_time = dateutils.add_intervals_unix(end_time, interval, count)
            pricing = aggregate.get_interpolated_pricing(ticker, start_time, end_time, interval, return_quotes=QUOTES, max_fill_ratio=0.2)
            if len(pricing[0]) != count:
                raise Exception(f"Unexpected number of timestamps for start_time {start_time} end time {end_time} interval {interval} count {count}. Got {len(data[interval])}.")
            after_data[f"{OUTPUT_KEY_PREFIX}_{interval.name}"] = torch.stack([torch.tensor(it, dtype=torch.float64) for it in data], dim=1)
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

