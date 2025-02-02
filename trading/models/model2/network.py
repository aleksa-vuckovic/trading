import torch
import math
import torchinfo
import config
from ..model1 import example

class RecursiveLayer(torch.nn.Module):
    def __init__(self, in_features: int, out_features: int, time_steps: int):
        super().__init__()
        self.in_features = in_features
        self.out_features = out_features
        self.time_steps = time_steps
        self.layer = torch.nn.RNN(input_size=in_features, hidden_size=out_features, num_layers=2, batch_first=True)
    
    def forward(self, series: torch.Tensor):
        series = series.transpose(1,2)
        return self.layer(series)[1][-1]

class ConvolutionalLayer(torch.nn.Module):
    def __init__(self, input_features: int, output_features: int = 10):
        super().__init__()
        self.layer = torch.nn.Sequential(
            torch.nn.Conv1d(in_channels = input_features, out_channels = 5, kernel_size = 5, stride = 1, padding=2),
            torch.nn.ReLU(),
            torch.nn.Conv1d(in_channels = 5, out_channels = 5, kernel_size = 5, stride = 1, padding=2),
            torch.nn.ReLU(),
            torch.nn.Conv1d(in_channels = 5, out_channels = output_features, kernel_size=5, stride = 1, padding=2)
        )

    def forward(self, series):
        return self.layer(series)

class Model(torch.nn.Module):
    """64 layers"""
    def __init__(self):
        super().__init__()
        self.daily_conv = torch.nn.Sequential(
            ConvolutionalLayer(input_features=3, output_features=10),
            RecursiveLayer(in_features=10, out_features=10, time_steps=example.D1_PRICES)
        )
        self.hourly_conv = torch.nn.Sequential(
            ConvolutionalLayer(input_features=3, output_features=10),
            RecursiveLayer(in_features=10, out_features=10, time_steps=example.H1_PRICES)
        )
        self.daily = RecursiveLayer(in_features=3, out_features=10, time_steps=example.D1_PRICES)
        self.hourly = RecursiveLayer(in_features=3, out_features=10, time_steps=example.H1_PRICES)

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
        input = [(config.batch_size, example.D1_PRICES)]*2
        input += [(config.batch_size, example.H1_PRICES)]*2
        torchinfo.summary(model, input_size=input)


