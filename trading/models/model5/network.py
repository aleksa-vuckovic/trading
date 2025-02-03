import torch
import torchinfo
from torch import Tensor
import config
from ..model2.network import RecursiveLayer, ConvolutionalLayer
from ..utils import relativize_in_place, PriceTarget
from . import example

TOTAL_POINTS = 100
INPUT_FEATURES = 6

class Model(torch.nn.Module):
    """64 layers"""
    def __init__(self):
        super().__init__()
        self.daily_conv = torch.nn.Sequential(
            ConvolutionalLayer(input_features=INPUT_FEATURES, output_features=2*INPUT_FEATURES),
            RecursiveLayer(in_features=2*INPUT_FEATURES, out_features=4*INPUT_FEATURES)
        )
        self.hourly_conv = torch.nn.Sequential(
            ConvolutionalLayer(input_features=INPUT_FEATURES, output_features=2*INPUT_FEATURES),
            RecursiveLayer(in_features=2*INPUT_FEATURES, out_features=4*INPUT_FEATURES)
        )
        self.daily = RecursiveLayer(in_features=INPUT_FEATURES, out_features=2*INPUT_FEATURES)
        self.hourly = RecursiveLayer(in_features=INPUT_FEATURES, out_features=2*INPUT_FEATURES)

        self.dense = torch.nn.Sequential(
            torch.nn.Linear(in_features=12*INPUT_FEATURES, out_features=5*INPUT_FEATURES),
            torch.nn.Sigmoid(),
            torch.nn.Linear(in_features=5*INPUT_FEATURES, out_features=10),
            torch.nn.Sigmoid(),
            torch.nn.Linear(in_features=10, out_features=1),
            torch.nn.Tanh()
        )

    def forward(self, daily, hourly):
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
        input = [(config.batch_size*10, INPUT_FEATURES, TOTAL_POINTS)]*2
        torchinfo.summary(model, input_size=input)

def extract_tensors(batch: dict[str, Tensor]) -> tuple[Tensor, Tensor, Tensor]:
    daily = batch[example.D1_DATA]
    hourly = batch[example.H1_DATA]
    after = batch[example.AFTER_DATA]

    after = (after[:,example.D1_AFTER_I] - daily[:,-1,example.CLOSE_I]) / daily[:,-1,example.CLOSE_I]
    after = PriceTarget.TANH_10_10.get_price(after)
    def process(tensor: Tensor):
        tensor = tensor[:,-TOTAL_POINTS,:]
        #1 Append high-low
        tensor = torch.concat([tensor, (tensor[:,:,example.HIGH_I] - tensor[:,:,example.LOW_I]).unsqueeze(dim=2)], dim=2)
        #2 Relativize open to close
        tensor[:,:,example.OPEN_I] = (tensor[:,:,example.CLOSE_I] - tensor[:,:,example.OPEN_I]) / tensor[:,:,example.OPEN_I]
        #3 Time relativize close, low, high, volume, high-low
        relativize_in_place(tensor[:,:,1:], dim=1)
        return tensor.transpose(1,2)
    daily = process(daily)
    hourly = process(hourly)
    return daily, hourly, after