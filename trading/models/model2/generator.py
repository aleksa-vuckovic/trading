import logging
import matplotlib.axes
import torch
import os
import random
import matplotlib
from torch import Tensor
from pathlib import Path
from matplotlib import pyplot as plt
from ...utils import dateutils
from ...utils.common import Interval
from ...data import aggregate, nasdaq
from ..utils import check_tensor, PriceTarget
from ..abstract import ExampleGenerator

logger = logging.getLogger(__name__)

DATA_POINTS = 300
OPEN_I = 0
CLOSE_I = 1
LOW_I = 2
HIGH_I = 3
VOLUME_I = 4
AFTER_D1_I = 0
AFTER_D2_I = 6
AFTER_D5_I = 12
AFTER_HIGH_OFF = 0
AFTER_LOW_OFF = 1
AFTER_LOWHIGH_OFF = 2
AFTER_CLOSE_OFF = 3
AFTER_CLOSEHIGH_OFF = 4
AFTER_CLOSELOW_OFF = 5
D1_DATA = 'd1_data'
H1_DATA = 'h1_data'
AFTER_DATA = 'after_data'

FOLDER = Path(__file__).parent / 'examples'

class Generator(ExampleGenerator):
    def run(self):
        for hour in [12, 15, 13, 14]:
            logger.info(f"-------------Starting loop for {hour}----------------")
            self._run_loop(
                folder = FOLDER,
                hour = hour
            )
    def generate_example(
        self,
        ticker: nasdaq.NasdaqListedEntry,
        end_time: float,
        with_output: bool = True
    ) -> dict[str, Tensor]:
        #1. Get the prices
        d1_start_time = (end_time-(DATA_POINTS/5*7*1.5+5)*24*3600)
        h1_start_time = (end_time-(DATA_POINTS/6/5*7*1.5+5)*24*3600)
        quotes = ['open', 'close', 'low', 'high', 'volume']
        d1_data = aggregate.get_pricing(ticker, d1_start_time, end_time, Interval.D1, return_quotes=quotes)
        if len(d1_data[0]) < DATA_POINTS:
            raise Exception(f'Failed to fetch enough daily prices for {ticker.symbol}. Got {len(d1_data[0])}')
        h1_data = aggregate.get_pricing(ticker, h1_start_time, end_time, Interval.H1, return_quotes=quotes)
        if len(h1_data[0]) < DATA_POINTS:
            raise Exception(f'Failed to fetch enough hourly prices for {ticker.symbol}. Got {len(h1_data[0])}')
        
        d1_data = torch.stack([torch.tensor(it[-DATA_POINTS:], dtype=torch.float64) for it in d1_data], dim=1)
        h1_data = torch.stack([torch.tensor(it[-DATA_POINTS:], dtype=torch.float64) for it in h1_data], dim=1)
        check_tensor(d1_data, allow_zeros=False)
        check_tensor(h1_data, allow_zeros=False)
        if not with_output:
            return {D1_DATA: d1_data, H1_DATA: h1_data}

        d1_after_high, d1_after_low, d1_after_close = aggregate.get_pricing(ticker, end_time, dateutils.add_business_days_unix(end_time, 1, tz=dateutils.ET), Interval.H1, return_quotes=['high', 'low', 'close'])
        d2_after_high, d2_after_low, d2_after_close = aggregate.get_pricing(ticker, end_time, dateutils.add_business_days_unix(end_time, 2, tz=dateutils.ET), Interval.H1, return_quotes=['high', 'low', 'close'])
        d5_after_high, d5_after_low, d5_after_close = aggregate.get_pricing(ticker, end_time, dateutils.add_business_days_unix(end_time, 5, tz=dateutils.ET), Interval.D1, return_quotes=['high', 'low', 'close'])
        if len(d1_after_high) < 3:
            raise Exception(f"Failed to fetch enough hourly after prices for {ticker.symbol}. Got {len(d1_after_high)}.")
        if len(d2_after_high) < 6:
            raise Exception(f"Failed to fetch enough hourly after prices for {ticker.symbol}. Got {len(d2_after_high)}.")
        if len(d5_after_high) < 3:
            raise Exception(f"Failed to fetch enough daily after prices for {ticker.symbol}. Got {len(d5_after_high)}.")
        after_data = torch.tensor([
            max(d1_after_high), min(d1_after_low), max(d1_after_low), d1_after_close[-1], max(d1_after_close), min(d1_after_close),
            max(d2_after_high), min(d2_after_low), max(d2_after_low), d2_after_close[-1], max(d2_after_close), min(d2_after_close),
            max(d5_after_high), min(d5_after_low), max(d5_after_low), d5_after_close[-1], max(d5_after_close), min(d5_after_close),
        ], dtype=torch.float64)
        check_tensor(after_data, allow_zeros=False)
        return {D1_DATA: d1_data, H1_DATA: h1_data, AFTER_DATA: after_data}

    def plot_statistics(self, target: PriceTarget = PriceTarget.TANH_10_10, index: int = AFTER_CLOSE_OFF):
        #Bin distribution of after values
        temp = []
        files = [FOLDER/it for it in os.listdir(FOLDER)]
        random.shuffle(files)
        for file in files[:20]:
            batch = torch.load(file, weights_only=True)
            data = (batch[AFTER_DATA] - batch[H1_DATA][:,-1:,CLOSE_I])/(batch[H1_DATA][:,-1:,CLOSE_I])
            temp.append(data)
        data = torch.concat(temp, dim=0)
        d1_data = data[:,AFTER_D1_I+index]
        d2_data = data[:,AFTER_D2_I+index]
        d5_data = data[:,AFTER_D5_I+index]
        for data, name in [(d1_data, 'D1'), (d2_data, 'D2'), (d5_data, 'D5')]:
            fig, axes = plt.subplots(1, 2)
            axes: list[matplotlib.axes.Axes] = axes
            fig.suptitle(name)

            axes[0].set_title('Raw')
            axes[0].set_xlabel('Percentage change')
            axes[0].set_ylabel('Number of examples')
            axes[0].hist(data*100, bins=range(-20,21,1), edgecolor='black')

            axes[1].set_title(target.name)
            axes[1].set_xlabel('Expected output')
            axes[1].set_ylabel('Number of examples')
            axes[1].hist(target.get_price(data), bins=20, edgecolor='black')
            
        plt.show()

