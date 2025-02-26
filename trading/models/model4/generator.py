
import logging
import torch
import matplotlib
import random
import time
import os
from torch import Tensor
from pathlib import Path
from typing import Callable
from matplotlib import pyplot as plt
from ...utils import dateutils
from ...utils.dateutils import TimingConfig
from ...utils.common import Interval
from ...data import nasdaq, aggregate
from ..abstract import ExampleGenerator, PriceEstimator, QUOTES, CLOSE_I, OUTPUT_KEY_PREFIX
from ..utils import check_tensors, PriceTarget


logger = logging.getLogger(__name__)

FOLDER = Path(__file__).parent / 'examples'

class Generator(ExampleGenerator):
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

        max_interval = sorted(data_points.keys())[-1]
        min_interval = sorted(data_points.keys())[0]
        after_max_interval = sorted(after_data_points.keys())[-1]

        self.tickers = [
            (
                it,
                dateutils.add_intervals_unix(
                    aggregate.get_first_trade_time(it),
                    max_interval,
                    data_points[max_interval]
                )
            )
            for it in aggregate.get_sorted_tickers()
        ]
        self.time_frame = (
            dateutils.add_intervals_unix(min_interval.start_unix(), min_interval, data_points[min_interval]),
            dateutils.add_intervals_unix(time.time(), after_max_interval, -after_data_points[after_max_interval])
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
        target: PriceTarget = PriceTarget.TANH_10_10,
        title: str = ""
    ):
        #Bin distribution of after values
        temp = []
        files = [FOLDER/it for it in os.listdir(FOLDER)]
        random.shuffle(files)
        for file in files[:20]:
            batch = torch.load(file, weights_only=True)
            close = batch[Interval.H1.name][:,-1:,CLOSE_I]
            data = (estimator.estimate_example(batch) - close)/close
            temp.append(data)
        data = torch.concat(temp, dim=0)
        fig, axes = plt.subplots(1, 2, figsize=(5,6))
        fig.suptitle(title)
        axes: list[matplotlib.axes.Axes] = axes
        
        axes[0].set_title(f'Raw')
        axes[0].set_xlabel('Percentage change')
        axes[0].hist(data*100, bins=range(-20,21,1), edgecolor='black')

        axes[1].set_title(f"{target.name}")
        axes[1].set_xlabel('Expected output')
        axes[1].hist(target.get_price(data), bins=20, edgecolor='black')
        fig.tight_layout()
        plt.show()

