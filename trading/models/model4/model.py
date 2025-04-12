from itertools import chain
from typing import Literal, overload, override
import torch
import torchinfo
import config
from torch import Tensor
from storage import PricingDataConfig, TimingConfig
from trading.models.base.model_config import BaseModelConfig, PriceEstimator, PriceTarget, BarValues
from trading.models.base.abstract_model import AbstractModel
from trading.models.base.tensors import get_moving_average, get_time_relativized, check_tensors, check_tensor

class ModelConfig(BaseModelConfig):
    def __init__(
        self,
        estimator: PriceEstimator,
        target:  PriceTarget,
        timing: TimingConfig,
        pricing_data_config: PricingDataConfig,
        mvg_window: int = 10
    ):
        super().__init__(pricing_data_config, estimator, target, timing)
        self.mvg_window = mvg_window

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
        super().__init__(config)
        self.config = config
        self.input_features = 10

        self.conv_layers = torch.nn.ModuleDict(
            {
                interval.name : torch.nn.Sequential(
                    ConvolutionalLayer(input_features=self.input_features, output_features=10*self.input_features),
                    torch.nn.BatchNorm1d(num_features=10*self.input_features),
                    RecursiveLayer(in_features=10*self.input_features, out_features=10*self.input_features)
                ) for interval, count in self.config.pricing_data_config.counts.items()
            }
        )
        self.layers = torch.nn.ModuleDict(
            {
                interval.name : RecursiveLayer(in_features=self.input_features, out_features=10*self.input_features)
                for interval, count in self.config.pricing_data_config.counts.items()
            }
        )

        total_features = 20*self.input_features*len(self.config.pricing_data_config)
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

    #region Overrides
    @override
    def predict(self, example: dict[str, Tensor]) -> Tensor:
        output = torch.cat(
            [
                *(self.conv_layers[interval.name](example[interval.name]) for interval in self.config.pricing_data_config.intervals),
                *(self.layers[interval.name](example[interval.name]) for interval in self.config.pricing_data_config.intervals)
            ],
            dim=1
        )  
        return self.dense(output)
    @overload
    def extract_tensors(self, example: dict[str, Tensor], with_output: Literal[False]=...) -> dict[str,Tensor]: ...
    @overload
    def extract_tensors(self, example: dict[str, Tensor], with_output: Literal[True]) -> tuple[dict[str,Tensor],Tensor]: ...
    @override
    def extract_tensors(self, example: dict[str, Tensor], with_output: bool = False) -> dict[str, Tensor]|tuple[dict[str, Tensor], Tensor]:
        if len(next(iter(example.values())).shape) < 3: # Make sure there's a batch dimension
            example = { key: example[key].unsqueeze(dim=0) for key in example }

        def process(tensor: Tensor, count: int):
            tensor = tensor[:,-count-self.config.mvg_window:,:5]
            #1 Get high-low relative to low (relative span)
            tensor[:,:,BarValues.O.value] = (tensor[:,:,BarValues.H.value] - tensor[:,:,BarValues.L.value]) / tensor[:,:,BarValues.L.value]
            #2 Get moving averages for everything
            mvg = get_moving_average(tensor, dim=1, window=self.config.mvg_window)
            #3 Concat everything
            tensor = torch.concat([tensor, mvg], dim=2)
            #4 Relativize all except relative span
            tensor[:,:,1:5] = get_time_relativized(tensor[:,:,1:5], dim=1)
            tensor[:,:,6:10] = get_time_relativized(tensor[:,:,6:10], dim=1)
            return tensor[:,-count:,:].transpose(1,2)
        
        tensors = { 
            key : process(example[key], self.config.pricing_data_config[key])
            for key in example if key in {it.name for it in self.config.pricing_data_config.intervals}
        }
        check_tensors(tensors)
        if with_output:
            after = self.config.estimator.estimate_example(example)
            close = example[self.config.pricing_data_config.min_interval.name][:,-1,BarValues.C.value]
            after = (after[:,-1] - close) / close
            after = self.config.target.get_price(after)
            check_tensor(after)
            return tensors, after
        return tensors

    #endregion