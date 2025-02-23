import torch
import torchinfo
import config
from torch import Tensor
from ...utils.common import Interval
from ..abstract import AbstractModel, ModelConfig, OPEN_I, HIGH_I, LOW_I, CLOSE_I, VOLUME_I
from ..utils import get_moving_average, get_time_relativized, check_tensors

INPUT_FEATURES = 'input_features'
D1_DATA_POINTS = 'd1_data_points'
H1_DATA_POINTS = 'h1_data_points'
MVG_WINDOW = 'mvg_window'

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
    """
    Predict price change based on OHLCV time series (daily an hourly),
    and their 10-data-point moving average counterparts.
    Configurable with regards to the prediction interval, timing and output.
    """
    def __init__(self, config: ModelConfig):
        config.data.setdefault(INPUT_FEATURES, 10)
        config.data.setdefault(D1_DATA_POINTS, 50)
        config.data.setdefault(H1_DATA_POINTS, 100)
        config.data.setdefault(MVG_WINDOW, 10)
        super().__init__(config)
        input_features = self.data[INPUT_FEATURES]
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
            self.config.output.value
        )

    def forward(self, daily, hourly):
        output = torch.cat([
          self.daily_conv(daily),
          self.daily(daily),
          self.hourly_conv(hourly),
          self.hourly(hourly)  
        ], dim=1)
        return self.dense(output)

    def extract_tensors(self, example: dict[str, Tensor]):
        if len(example[Interval.D1.name].shape) < 3:
            example = {key: example[key].unsqueeze(dim=0) for key in example}
        daily_raw = example[Interval.D1.name]
        hourly_raw = example[Interval.H1.name]

        def process(tensor: Tensor, data_points: int):
            tensor = tensor[:,-data_points-self.config.data[MVG_WINDOW]:,:]
            #1 Get high-low relative to low (relative span)
            tensor[:,:,OPEN_I] = (tensor[:,:,HIGH_I] - tensor[:,:,LOW_I]) / tensor[:,:,LOW_I]
            #2 Get moving averages for everything
            mvg = get_moving_average(tensor, dim=1, window=self.config.data[MVG_WINDOW])
            #3 Concat everything
            tensor = torch.concat([tensor, mvg], dim=2)
            #4 Relativize all except relative span
            tensor[:,:,1:5] = get_time_relativized(tensor[:,:,1:5], dim=1)
            tensor[:,:,6:10] = get_time_relativized(tensor[:,:,6:10], dim=1)
            return tensor[:,-data_points:,:].transpose(1,2)
        daily = process(daily_raw, self.config.data[D1_DATA_POINTS])
        hourly = process(hourly_raw, self.config.data[H1_DATA_POINTS])
        result = (daily, hourly)

        if len(example) > 2:
            after = self.config.estimator.estimate_example(example)
            close = hourly_raw[:,-1,CLOSE_I]
            after = (after[:,-1] - close) / close
            after = self.config.target.get_price(after)
            result += (after,)

        check_tensors(result)
        return result

    def print_summary(self, merge:int = 10):
        input = [
            (config.batch_size*merge, self.config.data[INPUT_FEATURES], self.config.data[D1_DATA_POINTS]),
            (config.batch_size*merge, self.config.data[INPUT_FEATURES], self.config.data[H1_DATA_POINTS])
        ]
        torchinfo.summary(self, input_size=input)