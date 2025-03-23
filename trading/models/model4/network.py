from typing import Literal, overload, override
import torch
import torchinfo
import config
from torch import Tensor
from trading.core import Interval
from trading.models.abstract import AbstractModel, ModelConfig, Quote
from trading.models.utils import get_moving_average, get_time_relativized, check_tensors, check_tensor

INPUT_FEATURES = 'input_features'
MVG_WINDOW = 'mvg_window'

class RecursiveLayer(torch.nn.Module):
    def __init__(self, in_features: int, out_features: int):
        super().__init__()
        self.in_features = in_features
        self.out_features = out_features
        self.layer = torch.nn.GRU(input_size =in_features, hidden_size=out_features, num_layers=2, batch_first=True)

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
        config.other.setdefault(INPUT_FEATURES, 10)
        config.other.setdefault(MVG_WINDOW, 10)
        super().__init__(config)
        input_features = self.config.other[INPUT_FEATURES]

        self.conv_layers = torch.nn.ModuleDict(
            {
                interval.name : torch.nn.Sequential(
                    ConvolutionalLayer(input_features=input_features, output_features=10*input_features),
                    torch.nn.BatchNorm1d(num_features=10*input_features),
                    RecursiveLayer(in_features=10*input_features, out_features=10*input_features)
                ) for interval, count in self.config.data_config 
            }
        )
        self.layers = torch.nn.ModuleDict(
            {
                interval.name : RecursiveLayer(in_features=input_features, out_features=10*input_features)
                for interval, count in self.config.data_config
            }
        )

        total_features = 20*input_features*len(self.config.data_config)
        self.dense = torch.nn.Sequential(
            torch.nn.BatchNorm1d(num_features=total_features),
            torch.nn.Linear(in_features=total_features, out_features=total_features//5),
            torch.nn.Sigmoid(),
            torch.nn.Linear(in_features=total_features//5, out_features=total_features//20),
            torch.nn.Sigmoid(),
            torch.nn.BatchNorm1d(num_features=total_features//20),
            torch.nn.Linear(in_features=total_features//20, out_features=1),
            self.config.target.get_layer()
        )

    @override
    def forward(self, tensors: dict[str, Tensor]) -> Tensor:
        output = torch.cat(
            [
                *(self.conv_layers[interval](tensors[interval]) for interval in tensors),
                *(self.layers[interval](tensors[interval]) for interval in tensors)
            ],
            dim=1
        )  
        return self.dense(output)

    @overload
    def extract_tensors(self, example: dict[str, Tensor], with_output: Literal[False]) -> dict[str,Tensor]: ...
    @overload
    def extract_tensors(self, example: dict[str, Tensor], with_output: Literal[True]=...) -> tuple[dict[str,Tensor],Tensor]: ...
    @override
    def extract_tensors(self, example: dict[str, Tensor], with_output: bool = True) -> dict[str, Tensor]|tuple[dict[str, Tensor], Tensor]:
        if len(next(iter(example.values())).shape) < 3: # Make sure there's a batch dimension
            example = { key: example[key].unsqueeze(dim=0) for key in example }

        def process(tensor: Tensor, count: int):
            tensor = tensor[:,-count-self.config.other[MVG_WINDOW]:,:]
            #1 Get high-low relative to low (relative span)
            tensor[:,:,Quote.O.value] = (tensor[:,:,Quote.H.value] - tensor[:,:,Quote.L.value]) / tensor[:,:,Quote.L.value]
            #2 Get moving averages for everything
            mvg = get_moving_average(tensor, dim=1, window=self.config.other[MVG_WINDOW])
            #3 Concat everything
            tensor = torch.concat([tensor, mvg], dim=2)
            #4 Relativize all except relative span
            tensor[:,:,1:5] = get_time_relativized(tensor[:,:,1:5], dim=1)
            tensor[:,:,6:10] = get_time_relativized(tensor[:,:,6:10], dim=1)
            return tensor[:,-count:,:].transpose(1,2)
        
        tensors = { 
            key : process(example[key], self.config.data_config[key])
            for key in example if key in self.config.data_config
        }
        check_tensors(tensors)
        if with_output:
            after = self.config.estimator.estimate_example(example)
            close = example[self.config.data_config.min_interval.name][:,-1,Quote.C.value]
            after = (after[:,-1] - close) / close
            after = self.config.target.get_price(after)
            check_tensor(after)
            return tensors, after
        return tensors

    def print_summary(self, merge:int = 10):
        input = [
            (config.models.batch_size*merge, self.config.other[INPUT_FEATURES], count)
            for interval, count in self.config.data_config
        ]
        torchinfo.summary(self, input_size=input)