import torch
import torchinfo
from torch import Tensor
import config
from ..model2.network import RecursiveLayer, ConvolutionalLayer
from ..utils import relativize_in_place, PriceTarget, check_tensors
from . import example

TOTAL_POINTS = 100
INPUT_FEATURES = 6

class Model(torch.nn.Module):
    """64 layers"""
    def __init__(self):
        super().__init__()
        self.daily_conv = torch.nn.Sequential(
            ConvolutionalLayer(input_features=INPUT_FEATURES, output_features=2*INPUT_FEATURES),
            torch.nn.BatchNorm1d(num_features=2*INPUT_FEATURES),
            RecursiveLayer(in_features=2*INPUT_FEATURES, out_features=4*INPUT_FEATURES)
        )
        self.hourly_conv = torch.nn.Sequential(
            ConvolutionalLayer(input_features=INPUT_FEATURES, output_features=2*INPUT_FEATURES),
            torch.nn.BatchNorm1d(num_features=2*INPUT_FEATURES),
            RecursiveLayer(in_features=2*INPUT_FEATURES, out_features=4*INPUT_FEATURES)
        )
        self.daily = RecursiveLayer(in_features=INPUT_FEATURES, out_features=2*INPUT_FEATURES)
        self.hourly = RecursiveLayer(in_features=INPUT_FEATURES, out_features=2*INPUT_FEATURES)

        self.dense = torch.nn.Sequential(
            torch.nn.BatchNorm1d(num_features=12*INPUT_FEATURES),
            torch.nn.Linear(in_features=12*INPUT_FEATURES, out_features=5*INPUT_FEATURES),
            torch.nn.Sigmoid(),
            torch.nn.Linear(in_features=5*INPUT_FEATURES, out_features=10),
            torch.nn.Sigmoid(),
            torch.nn.BatchNorm1d(num_features=2*INPUT_FEATURES),
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
    
    @staticmethod
    def print_summary():
        model = Model()
        input = [(config.batch_size*10, INPUT_FEATURES, TOTAL_POINTS)]*2
        torchinfo.summary(model, input_size=input)

