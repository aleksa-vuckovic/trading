import torch
import torchinfo
import config
from torch import Tensor
from ..utils import PriceTarget
from ..abstract import AbstractModel
from ..utils import get_moving_average, get_time_relativized, check_tensors
from .generator import D1_DATA, H1_DATA, AFTER_D1_DATA, AFTER_H1_DATA
from ..abstract import OPEN_I, HIGH_I, LOW_I, CLOSE_I, VOLUME_I

class RecursiveLayer(torch.nn.Module):
    def __init__(self, in_features: int, out_features: int):
        super().__init__()
        self.in_features = in_features
        self.out_features = out_features
        self.layer = torch.nn.GRU(input_size =in_features, hidden_size=out_features, num_layers=3, batch_first=True)

    def forward(self, series: torch.Tensor):
        return self.layer(series.transpose(1,2))[1][-1]

class ConvolutionalLayer(torch.nn.Module):
    def __init__(self, input_features: int, output_features: int = 10):
        super().__init__()
        self.layer = torch.nn.Sequential(
            torch.nn.Conv1d(in_channels = input_features, out_channels = input_features*5, kernel_size = 5, stride = 1, padding=2),
            torch.nn.ReLU(),
            torch.nn.Conv1d(in_channels = input_features*5, out_channels = input_features*10, kernel_size = 5, stride = 1, padding=2),
            torch.nn.ReLU(),
            torch.nn.Conv1d(in_channels = input_features*10, out_channels = output_features, kernel_size=5, stride = 1, padding=2),
            torch.nn.ReLU()
        )
    def forward(self, series):
        return self.layer(series)

class Model(AbstractModel):
    """64 layers"""
    def __init__(self, input_features: int = 10, d1_data_points = 50, h1_data_points: int = 100, mvg_window: int = 10,  is_tanh: bool = False):
        super().__init__()
        self.input_features = input_features
        self.d1_data_points = d1_data_points
        self.h1_data_points = h1_data_points
        self.mvg_window = mvg_window
        self.daily_conv = torch.nn.Sequential(
            ConvolutionalLayer(input_features=input_features, output_features=10*input_features),
            torch.nn.BatchNorm1d(num_features=10*input_features),
            RecursiveLayer(in_features=10*input_features, out_features=10*input_features)
        )
        self.hourly_conv = torch.nn.Sequential(
            ConvolutionalLayer(input_features=input_features, output_features=10*input_features),
            torch.nn.BatchNorm1d(num_features=10*input_features),
            RecursiveLayer(in_features=10*input_features, out_features=10*input_features)
        )
        self.daily = RecursiveLayer(in_features=input_features, out_features=10*input_features)
        self.hourly = RecursiveLayer(in_features=input_features, out_features=10*input_features)

        self.dense = torch.nn.Sequential(
            torch.nn.BatchNorm1d(num_features=40*input_features),
            torch.nn.Linear(in_features=40*input_features, out_features=10*input_features),
            torch.nn.Sigmoid(),
            torch.nn.Linear(in_features=10*input_features, out_features=3*input_features),
            torch.nn.Sigmoid(),
            torch.nn.BatchNorm1d(num_features=3*input_features),
            torch.nn.Linear(in_features=3*input_features, out_features=1),
            torch.nn.Tanh() if is_tanh else torch.nn.Sigmoid()
        )

    def forward(self, daily, hourly):
        output = torch.cat([
          self.daily_conv(daily),
          self.daily(daily),
          self.hourly_conv(hourly),
          self.hourly(hourly)  
        ], dim=1)
        return self.dense(output)

    def extract_tensors(self, example):
        return self.extract_tensors_impl(example, target=PriceTarget.TANH_10_10)

    def extract_tensors_impl(self, example: dict[str, Tensor], target: PriceTarget):
        if len(example[D1_DATA].shape) < 3:
            example = {key: example[key].unsqueeze(dim=0) for key in example}
        daily_raw = example[D1_DATA]
        hourly_raw = example[H1_DATA]

        def process(tensor: Tensor, data_points: int):
            tensor = tensor[:,-data_points-self.mvg_window:,:]
            #1 Get high-low relative to low (relative span)
            tensor[:,:,OPEN_I] = (tensor[:,:,HIGH_I] - tensor[:,:,LOW_I]) / tensor[:,:,LOW_I]
            #2 Get moving averages for everything
            mvg = get_moving_average(tensor, dim=1, window=self.mvg_window)
            #3 Concat everything
            tensor = torch.concat([tensor, mvg], dim=2)
            #4 Relativize all except relative span
            tensor[:,:,1:5] = get_time_relativized(tensor[:,:,1:5], dim=1)
            tensor[:,:,6:10] = get_time_relativized(tensor[:,:,6:10], dim=1)
            return tensor[:,-data_points:,:].transpose(1,2)
        daily = process(daily_raw, self.d1_data_points)
        hourly = process(hourly_raw, self.h1_data_points)
        result = (daily, hourly)

        if AFTER_D1_DATA in example and AFTER_H1_DATA in example:
            after_h1 = example[AFTER_H1_DATA]
            after = (after_h1[:,-1,CLOSE_I] - hourly_raw[:,-1,CLOSE_I]) / hourly_raw[:,-1,CLOSE_I]
            after = target.get_price(after)
            result += (after,)

        check_tensors(result)
        return result

    def print_summary(self, merge:int = 10):
        input = [(config.batch_size*merge, self.input_features, self.data_points)]*2
        torchinfo.summary(self, input_size=input)

    #def get_metadata(self) -> ModelMetadata:
    #    return ModelMetadata(projection_period=1, description="""
    #        Predict price change on a tanh scale for the next business day,
    #        based on OHLCV time series (daily an hourly),
    #        and their 10-data-point moving average counterparts.
    #        The output should approach -1 for falls nearing 10 percent,
    #        and +1 for rises nearing 10 percent.
    #    """)