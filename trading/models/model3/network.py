import torch
import torchinfo
import config
from torch import Tensor
from .. import model2
from ..utils import PriceTarget, get_moving_average, get_time_relativized, check_tensors, check_tensor
from ..abstract import ModelMetadata

TOTAL_POINTS = 100
INPUT_FEATURES = 10
MOVING_AVG = 10
class Model(model2.network.Model):
    def __init__(self):
        super().__init__(input_features=INPUT_FEATURES, is_tanh=False)

    def extract_tensors(self, example: dict[str, Tensor]):
        if len(example[model2.generator.D1_DATA].shape) < 3:
            example = {key: example[key].unsqueeze(dim=0) for key in example}
        daily_raw = example[model2.generator.D1_DATA]
        hourly_raw = example[model2.generator.H1_DATA]

        OPEN = model2.generator.OPEN_I
        CLOSE = model2.generator.CLOSE_I
        LOW = model2.generator.LOW_I
        HIGH = model2.generator.HIGH_I
        VOL = model2.generator.VOLUME_I

        def process(tensor: Tensor):
            tensor = tensor[:,-TOTAL_POINTS-MOVING_AVG:,:]
            #1 Get high-low relative to low (relative span)
            tensor[:,:,OPEN] = (tensor[:,:,HIGH] - tensor[:,:,LOW]) / tensor[:,:,LOW]
            #2 Get moving averages for everything
            mvg = get_moving_average(tensor, dim=1, window=MOVING_AVG)
            #3 Concat everything
            tensor = torch.concat([tensor, mvg], dim=2)
            #4 Relativize all except relative span
            tensor[:,:,1:5] = get_time_relativized(tensor[:,:,1:5], dim=1)
            tensor[:,:,6:10] = get_time_relativized(tensor[:,:,6:10], dim=1)
            return tensor[:,-TOTAL_POINTS:,:].transpose(1,2)
        daily = process(daily_raw)
        hourly = process(hourly_raw)
        result = (daily, hourly)

        if model2.generator.AFTER_DATA in example:
            after = example[model2.generator.AFTER_DATA]
            after = (after[:,model2.generator.D1_AFTER_I] - hourly_raw[:,-1,CLOSE]) / hourly_raw[:,-1,CLOSE]
            after = PriceTarget.SIGMOID_0_5.get_price(after)
            result += (after,)

        check_tensors(result)
        return result

    def print_summary(self, merge:int = 10):
        input = [(config.batch_size*merge, self.input_features, TOTAL_POINTS)]*2
        torchinfo.summary(self, input_size=input)

    def get_metadata(self) -> ModelMetadata:
        return ModelMetadata(projection_period=1, description="""
            Predict price change on a sigmoid scale for the next business day,
            based on OHLCV time series (daily an hourly),
            and their 10-data-point moving average counterparts.
            The output should approach 0 for non-rising prices and 1 for prices rising 5 percent or more.
        """)