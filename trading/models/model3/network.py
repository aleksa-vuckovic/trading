import torch
import torchinfo
import config
from torch import Tensor
from .. import model2
from ..model2.generator import D1_DATA, H1_DATA, AFTER_DATA, OPEN_I, CLOSE_I, LOW_I, HIGH_I, AFTER_D1_I, AFTER_CLOSEHIGH_OFF
from ..utils import PriceTarget, get_moving_average, get_time_relativized, check_tensors, check_tensor
#from ..abstract import ModelMetadata

TOTAL_POINTS = 100
INPUT_FEATURES = 10
MOVING_AVG = 10
class Model(model2.network.Model):
    def __init__(self):
        super().__init__(is_tanh=False)

    def extract_tensors(self, example):
        return self.extract_tensors_impl(example, target=PriceTarget.SIGMOID_0_5, result_offset=AFTER_D1_I+AFTER_CLOSEHIGH_OFF)

    #def get_metadata(self) -> ModelMetadata:
    #    return ModelMetadata(projection_period=1, description="""
    #        Predict price change on a sigmoid scale for the next business day,
    #        based on OHLCV time series (daily an hourly),
    #        and their 10-data-point moving average counterparts.
    #        The output should approach 0 for non-rising prices and 1 for prices rising up to 5 percent.
    #    """)