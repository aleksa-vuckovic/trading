import torch
import math
import torchinfo
import config
from ..model1 import example

TOTAL_D1 = 100
TOTAL_H1 = 100

#streams = [torch.cuda.Stream() for it in range(4)]

class RecursiveLayer(torch.nn.Module):
    def __init__(self, in_features: int, out_features: int):
        super().__init__()
        self.in_features = in_features
        self.out_features = out_features
        self.layer = torch.nn.RNN(input_size=in_features, hidden_size=out_features, num_layers=2, batch_first=True)

    def forward(self, series: torch.Tensor):
        series = series.transpose(1,2)
        return self.layer(series)[1][-1]

class ConvolutionalLayer(torch.nn.Module):
    def __init__(self, input_features: int, output_features: int = 10):
        super().__init__()
        self.layer = torch.nn.Sequential(
            torch.nn.Conv1d(in_channels = input_features, out_channels = input_features*3, kernel_size = 5, stride = 1, padding=2),
            torch.nn.ReLU(),
            torch.nn.Conv1d(in_channels = input_features*3, out_channels = input_features*3, kernel_size = 5, stride = 1, padding=2),
            torch.nn.ReLU(),
            torch.nn.Conv1d(in_channels = input_features*3, out_channels = output_features, kernel_size=5, stride = 1, padding=2),
            torch.nn.ReLU()
        )

    def forward(self, series):
        return self.layer(series)

class Model(torch.nn.Module):
    """64 layers"""
    def __init__(self):
        super().__init__()
        self.daily_conv = torch.nn.Sequential(
            ConvolutionalLayer(input_features=3, output_features=10),
            RecursiveLayer(in_features=10, out_features=10)
        )
        self.hourly_conv = torch.nn.Sequential(
            ConvolutionalLayer(input_features=3, output_features=10),
            RecursiveLayer(in_features=10, out_features=10)
        )
        self.daily = RecursiveLayer(in_features=3, out_features=10)
        self.hourly = RecursiveLayer(in_features=3, out_features=10)

        self.dense = torch.nn.Sequential(
            torch.nn.Linear(in_features=40, out_features=40),
            torch.nn.ReLU(),
            torch.nn.Linear(in_features=40, out_features=10),
            torch.nn.ReLU(),
            torch.nn.Linear(in_features=10, out_features=1),
            torch.nn.Tanh()
        )

    def forward(self, daily_p, daily_v, hourly_p, hourly_v):
        daily = torch.cat([
            torch.unsqueeze(daily_p, dim=1),
            torch.unsqueeze(daily_v, dim=1),
            torch.unsqueeze(daily_p*daily_v, dim=1)
        ], dim=1)
        hourly = torch.cat([
            torch.unsqueeze(hourly_p, dim=1),
            torch.unsqueeze(hourly_v, dim=1),
            torch.unsqueeze(hourly_p*hourly_v, dim=1)
        ], dim=1)

        """layers = [self.daily_conv, self.daily, self.hourly_conv, self.hourly]
        inputs = [daily, daily, hourly, hourly]
        outputs = []
        for i in range(4):
            with torch.cuda.stream(streams[i]):
                outputs.append(layers[i](inputs[i]))
        torch.cuda.synchronize()
        output = torch.cat(outputs, dim=1)"""
        output = torch.cat([
          self.daily_conv(daily),
          self.daily(daily),
          self.hourly_conv(hourly),
          self.hourly(hourly)  
        ], dim=1)
        return self.dense(output)
    
    @staticmethod
    def print_summary():
        model = Model()
        input = [(config.batch_size*10, TOTAL_D1)]*2
        input += [(config.batch_size*10, TOTAL_H1)]*2
        torchinfo.summary(model, input_size=input)


def extract_tensors(batch: torch.Tensor) -> torch.Tensor:
    daily_p = batch[:,example.D1_PRICES_I:example.D1_PRICES_I+example.D1_PRICES]
    daily_v = batch[:,example.D1_VOLUMES_I:example.D1_VOLUMES_I+example.D1_PRICES]
    hourly_p = batch[:,example.H1_PRICES_I:example.H1_PRICES_I+example.H1_PRICES]
    hourly_v = batch[:,example.H1_VOLUMES_I:example.H1_VOLUMES_I+example.H1_PRICES]
    expect = batch[:,example.D1_TARGET_I]
    expect = example.PriceTarget.TANH_10_10.get_price(expect)
    return daily_p[:,-TOTAL_D1:], daily_v[:,-TOTAL_D1:], hourly_p[:,-TOTAL_H1:], hourly_v[:,-TOTAL_H1:], expect