import torch
import math
import torchinfo
import config
from ..model1 import example

TOTAL_D1 = 100
TOTAL_H1 = 100

class Model(torch.nn.Module):
    """64 layers"""
    def __init__(self):
        super().__init__()
        features = 2*TOTAL_D1+2*TOTAL_H1
        self.layer= torch.nn.Sequential(
            torch.nn.Linear(in_features=features, out_features=features),
            torch.nn.LeakyReLU(),
            torch.nn.Linear(in_features=features, out_features=features),
            torch.nn.LeakyReLU(),
            torch.nn.Linear(in_features=features, out_features=100),
            torch.nn.LeakyReLU(),
            torch.nn.Linear(in_features=100, out_features=1),
            torch.nn.Tanh()
        )

    def forward(self, daily_p, daily_v, hourly_p, hourly_v):
        output = torch.cat([daily_p, daily_v, hourly_p, hourly_v], dim=1)
        return self.layer(output)
    
    @staticmethod
    def print_summary():
        model = Model()
        input = [(config.batch_size, TOTAL_D1)]*2
        input += [(config.batch_size, TOTAL_H1)]*2
        torchinfo.summary(model, input_size=input)


def extract_tensors(batch: torch.Tensor) -> torch.Tensor:
    daily_p = batch[:,example.D1_PRICES_I:example.D1_PRICES_I+example.D1_PRICES]
    daily_v = batch[:,example.D1_VOLUMES_I:example.D1_VOLUMES_I+example.D1_PRICES]
    hourly_p = batch[:,example.H1_PRICES_I:example.H1_PRICES_I+example.H1_PRICES]
    hourly_v = batch[:,example.H1_VOLUMES_I:example.H1_VOLUMES_I+example.H1_PRICES]
    expect = batch[:,example.D1_TARGET_I]
    expect = example.PriceTarget.TANH_10_10.get_price(expect)
    return daily_p[:,-TOTAL_D1:], daily_v[:,-TOTAL_D1:], hourly_p[:,-TOTAL_H1:], hourly_v[:,-TOTAL_H1:], expect