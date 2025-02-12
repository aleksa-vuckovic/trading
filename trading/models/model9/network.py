import torch
from torch import Tensor
from .. import model6
from ..abstract import TensorExtractor
from ..utils import PriceTarget, get_moving_average, get_time_relativized, check_tensors

TOTAL_POINTS = 100
INPUT_FEATURES = 10
MOVING_AVG = 10
class Model(model6.network.Model):
    def __init__(self):
        super().__init__(input_features=INPUT_FEATURES)

class Extractor(TensorExtractor):
    def extract_tensors(self, example):
        daily = example[model6.generator.D1_DATA]
        hourly = example[model6.generator.H1_DATA]

        def process(tensor: Tensor):
            tensor = tensor[:,-TOTAL_POINTS-MOVING_AVG:,:]
            #1 Get high-low relative to low (relative span)
            tensor[:,:,model6.generator.OPEN_I] = (tensor[:,:,model6.generator.HIGH_I] - tensor[:,:,model6.generator.LOW_I]) / tensor[:,:,model6.generator.LOW_I]
            #2 Get moving averages for everything
            mvg = get_moving_average(tensor, dim=1, window=MOVING_AVG)
            #3 Concat everything
            tensor = torch.concat([tensor, mvg], dim=1)
            #4 Relativize
            tensor = get_time_relativized(tensor, dim=1)
            return tensor[:,-INPUT_FEATURES:,:].transpose(1,2)
        daily = process(daily)
        hourly = process(hourly)
        result = (daily, hourly)

        if model6.generator.AFTER_DATA in example:
            after = example[model6.generator.AFTER_DATA]
            after = (after[:,model6.generator.D1_AFTER_I] - daily[:,-1,model6.generator.CLOSE_I]) / daily[:,-1,model6.generator.CLOSE_I]
            after = PriceTarget.TANH_10_10.get_price(after)
            result += (after,)

        check_tensors(result)
        return result



