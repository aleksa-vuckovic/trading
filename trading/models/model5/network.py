import torch
import torchinfo
from torch import Tensor
import config
from ..abstract import TensorExtractor
from ..utils import get_time_relativized, PriceTarget, check_tensors
from .import generator

TOTAL_POINTS = 100
INPUT_FEATURES = 6

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

class Extractor(TensorExtractor):
    def extract_tensors(self, example: dict[str, Tensor]) -> tuple[Tensor, ...]:
        daily = example[generator.D1_DATA]
        hourly = example[generator.H1_DATA]
        if len(daily.shape < 3):
            example = {key: example[key].unsqueeze(dim=0) for key in example}

        def process(tensor: Tensor):
            tensor = tensor[:,-TOTAL_POINTS:,:]
            #1 Append high-low relative to low
            relative_span = (tensor[:,:,generator.HIGH_I] - tensor[:,:,generator.LOW_I]) / tensor[:,:,generator.LOW_I]
            tensor = torch.concat([tensor, relative_span.unsqueeze(dim=2)], dim=2)
            #2 Relativize open to close
            tensor[:,:,generator.OPEN_I] = (tensor[:,:,generator.CLOSE_I] - tensor[:,:,generator.OPEN_I]) / tensor[:,:,generator.OPEN_I]
            #3 Time relativize close, low, high, volume
            tensor[:,:,1:-1] = get_time_relativized(tensor[:,:,1:-1], dim=1)
            return tensor.transpose(1,2)
        daily = process(daily)
        hourly = process(hourly)
        result = (daily, hourly)

        if generator.AFTER_DATA in example:
            after = example[generator.AFTER_DATA]
            after = (after[:,generator.D1_AFTER_I] - daily[:,-1,generator.CLOSE_I]) / daily[:,-1,generator.CLOSE_I]
            after = PriceTarget.TANH_10_10.get_price(after)
            result += (after,)

        check_tensors(result)
        return daily, hourly, after