
import logging
import torch
import matplotlib
import random
import os
from torch import Tensor
from pathlib import Path
from typing import Callable
from matplotlib import pyplot as plt
from ...utils import dateutils
from ...utils.common import Interval
from ...data import nasdaq, aggregate
from ..abstract import ExampleGenerator
from ..utils import check_tensors, PriceTarget


logger = logging.getLogger(__name__)

D1_DATA_POINTS = 120
H1_DATA_POINTS = 150
AFTER_D1_DATA_POINTS = 10
AFTER_H1_DATA_POINTS = 21

OPEN_I = 0
HIGH_I = 1
LOW_I = 2
CLOSE_I = 3
VOLUME_I = 4

D1_DATA = 'd1_data'
H1_DATA = 'h1_data'
AFTER_D1_DATA = 'after_d1_data'
AFTER_H1_DATA = 'after_h1_data'

QUOTES = ['open', 'high', 'low', 'close', 'volume']

FOLDER = Path(__file__).parent / 'examples'


class Generator(ExampleGenerator):
    def run(self):
        for hour in [11, 15, 13, 14]:
            logger.info(f"-------------Starting loop for {hour}----------------")
            self._run_loop(
                folder = FOLDER,
                hour = hour,
                start_time_offset=30*25*3600
            )
    def generate_example(
        self,
        ticker: nasdaq.NasdaqListedEntry,
        end_time: float,
        with_output: bool = True
    ) -> dict[str, Tensor]:
        #1. Get the prices
        d1_start_time = (end_time-(D1_DATA_POINTS/5*7+20)*24*3600)
        h1_start_time = (end_time-(H1_DATA_POINTS/7/5*7+20)*24*3600)
        d1_data = aggregate.get_interpolated_pricing(ticker, d1_start_time, end_time, Interval.D1, return_quotes=QUOTES, max_fill_ratio=1/5)
        if len(d1_data[0]) < D1_DATA_POINTS:
            raise Exception(f'Failed to fetch enough daily prices for {ticker.symbol}. Got {len(d1_data[0])}')
        h1_data = aggregate.get_interpolated_pricing(ticker, h1_start_time, end_time, Interval.H1, return_quotes=QUOTES, max_fill_ratio=2/7)
        if len(h1_data[0]) < H1_DATA_POINTS:
            raise Exception(f'Failed to fetch enough hourly prices for {ticker.symbol}. Got {len(h1_data[0])}')
        d1_data = torch.stack([torch.tensor(it[-D1_DATA_POINTS:], dtype=torch.float64) for it in d1_data], dim=1)
        h1_data = torch.stack([torch.tensor(it[-H1_DATA_POINTS:], dtype=torch.float64) for it in h1_data], dim=1)
        check_tensors([d1_data, h1_data], allow_zeros=False)
        if not with_output: return {D1_DATA: d1_data, H1_DATA: h1_data}

        after_d1_data = aggregate.get_interpolated_pricing(ticker, end_time, dateutils.add_business_days_unix(end_time, 10, tz=dateutils.ET), Interval.D1, return_quotes=QUOTES, max_fill_ratio=1/5)
        if len(after_d1_data[0]) != AFTER_D1_DATA_POINTS:
            raise Exception(f"Unexpected number of after d1 data points {len(after_d1_data[0])}")
        after_h1_data = aggregate.get_interpolated_pricing(ticker, end_time, dateutils.add_business_days_unix(end_time, 3, tz=dateutils.ET), Interval.H1, return_quotes=QUOTES, max_fill_ratio=2/7)
        if len(after_h1_data[0]) != AFTER_H1_DATA_POINTS:
            raise Exception(f"Unexpected number of after h1 data points {len(after_h1_data[0])}")
        after_d1_data = torch.stack([torch.tensor(it, dtype=torch.float64) for it in after_d1_data], dim=1)
        after_h1_data = torch.stack([torch.tensor(it, dtype=torch.float64) for it in after_h1_data], dim=1)
        check_tensors([after_d1_data, after_h1_data], allow_zeros=False)
        return {D1_DATA: d1_data, H1_DATA: h1_data, AFTER_D1_DATA: after_d1_data, AFTER_H1_DATA: after_h1_data}

    def plot_statistics(self,
        target: PriceTarget = PriceTarget.TANH_10_10,
        price_selector: Callable[[dict[str, Tensor]], Tensor] = lambda batch: batch[AFTER_H1_DATA][:,-1,CLOSE_I],
        title: str = ""
    ):
        #Bin distribution of after values
        temp = []
        files = [FOLDER/it for it in os.listdir(FOLDER)]
        random.shuffle(files)
        for file in files[:20]:
            batch = torch.load(file, weights_only=True)
            data = (price_selector(batch) - batch[H1_DATA][:,-1:,CLOSE_I])/(batch[H1_DATA][:,-1:,CLOSE_I])
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

