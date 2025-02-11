import torch
import torchinfo
import config
from torch import Tensor
from ..model5.network import RecursiveLayer, ConvolutionalLayer

TOTAL_POINTS = 100
INPUT_FEATURES = 6

class Model(torch.nn.Module):
    """64 layers"""
    def __init__(self, input_features: int = INPUT_FEATURES):
        super().__init__()
        self.input_features = input_features
        self.daily_conv = torch.nn.Sequential(
            ConvolutionalLayer(input_features=input_features, output_features=2*input_features),
            torch.nn.BatchNorm1d(num_features=2*input_features),
            RecursiveLayer(in_features=2*input_features, out_features=4*input_features)
        )
        self.hourly_conv = torch.nn.Sequential(
            ConvolutionalLayer(input_features=input_features, output_features=2*input_features),
            torch.nn.BatchNorm1d(num_features=2*input_features),
            RecursiveLayer(in_features=2*input_features, out_features=4*input_features)
        )
        self.daily = RecursiveLayer(in_features=input_features, out_features=2*input_features)
        self.hourly = RecursiveLayer(in_features=input_features, out_features=2*input_features)

        self.dense = torch.nn.Sequential(
            torch.nn.BatchNorm1d(num_features=12*input_features),
            torch.nn.Linear(in_features=12*input_features, out_features=5*input_features),
            torch.nn.Sigmoid(),
            torch.nn.Linear(in_features=5*input_features, out_features=10),
            torch.nn.Sigmoid(),
            torch.nn.BatchNorm1d(num_features=10, momentum=0.5),
            torch.nn.Linear(in_features=10, out_features=1),
            torch.nn.Tanh()
        )

    def forward(self, daily, hourly):
        output = torch.cat([
          self.daily_conv(daily),
          self.daily(daily),
          self.hourly_conv(hourly),
          self.hourly(hourly)  
        ], dim=1)
        return self.dense(output)
    
    def print_summary(self):
        input = [(config.batch_size*10, self.input_features, TOTAL_POINTS)]*2
        torchinfo.summary(self, input_size=input)

