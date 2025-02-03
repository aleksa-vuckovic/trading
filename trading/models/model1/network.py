import torch
import math
import torchinfo
import config
from ..utils import PriceTarget
from . import example
from . import generator

class IndividualTextLayer(torch.nn.Module):
    """
    4 layers, 64240 parameters
    ----------------------------
    2 linear+gelu layers, 768->80->40
    """
    def __init__(self, out_features = 40):
        super().__init__()
        self.dense1 = torch.nn.Linear(in_features = example.TEXT_EMBEDDING_SIZE, out_features=2*out_features) #80*768=61040
        self.gelu1 = torch.nn.GELU()
        self.dense2 = torch.nn.Linear(in_features=2*out_features, out_features=out_features) #80*40=3200
        self.gelu2 = torch.nn.GELU()
        self.norm = torch.nn.BatchNorm1d(num_features=out_features)

    def forward(self, input: torch.Tensor):
        return self.gelu2(self.dense2(self.gelu1(self.dense1(input))))
    
class CombinerLayer(torch.nn.Module):
    """
    4 layers, 2400 parameters
    ---------------------------
    2 linear+relu layers, 40->20, 80->20
        results are cross produced, and concatenated with the input -> 520
    """
    def __init__(
        self,
        in_features_left: int,
        in_features_right: int,
        reduced_features: int = 20,
        concat_input: bool = False
    ):
        super().__init__()
        self.left_reducer = torch.nn.Sequential(
            torch.nn.Linear(in_features=in_features_left, out_features=reduced_features), # 40*20
            torch.nn.GELU()
        )
        self.right_reducer = torch.nn.Sequential(
            torch.nn.Linear(in_features=in_features_right, out_features=reduced_features), # 80*20
            torch.nn.GELU()
        )
        self.concat_input = concat_input

    def forward(self, left, right):
        reduced_left = self.left_reducer(left)
        reduced_right = self.right_reducer(right)
        combined = reduced_left.unsqueeze(dim = 1) * reduced_right.unsqueeze(dim = 2)
        result = combined.flatten(start_dim = 1)
        if self.concat_input:
            return torch.concat([left, right, result], dim=1)
        else:
            return result

class CombinedTextLayer(torch.nn.Module):
    """
    6 layers, 54400 parameters
    ------------------------------
    1 combiner layer, 120->520
    1 liner+gelu, 520->100
    
    """
    def __init__(self, in_features: int = 40, out_features = 100):
        super().__init__()
        self.combiner = CombinerLayer(
            in_features_left=in_features,
            in_features_right=in_features*2,
            reduced_features=20,
            concat_input=True #400+120 = 520 out features
        )
        self.finalizer = torch.nn.Sequential(
            torch.nn.Linear(in_features=20**2+3*in_features, out_features=out_features),
            torch.nn.GELU(),
            torch.nn.BatchNorm1d(num_features=out_features)
        )

    def forward(self, text1, text2, text3):
        combined = self.combiner(text1, torch.concat([text2, text3], dim=1))
        return self.finalizer(combined)
    
class TextLayer(torch.nn.Module):
    """
    18 layers, 64240*3+54400=247120 parameters
    -----------------------------------------------------
    3 individual layers, 768->40
    1 combiner layer, 120->100
    """
    def __init__(self, individual_features: int = 40, out_features: int = 100):
        super().__init__()
        self.individual_layers = torch.nn.ModuleList([
            IndividualTextLayer(out_features=individual_features) for i in range(3)
        ])
        self.combiner = CombinedTextLayer(in_features=individual_features, out_features=out_features)
    
    def forward(self, text1, text2, text3):
        texts = [text1, text2, text3]
        texts = [self.individual_layers[i](texts[i]) for i in range(3)]
        return self.combiner(*texts)


class IndividualSeriesLayer(torch.nn.Module):
    """
    10 layer, 12+90+350=452...
    ----------------------------
    1 conv+relu, 1*x->5*(x-3)
    1 conv+relu, 5*(x-3)->10*(x-7)
    --
    1 avgpool,   10*(x-7)->10*((x-7)//5)
    1 conv+relu,  10*t->10*(t-5)
    1 avgpool,   10*(t-5)->10*((t-5)//5)
    1 conv+relu, 10-
    """
    def __init__(self, input_length: int, output_features: int = 100):
        super().__init__()
        self.conv1 = torch.nn.Sequential(
            torch.nn.Conv1d(in_channels = 1, out_channels = 5, kernel_size = 4, stride = 1),
            torch.nn.LeakyReLU(),
            torch.nn.BatchNorm1d(num_features=5),
            torch.nn.Conv1d(in_channels = 5, out_channels = 10, kernel_size = 5, stride = 1),
            torch.nn.LeakyReLU(),
            torch.nn.BatchNorm1d(num_features=10)
        )
        inner_features = input_length-3-4

        self.conv2 = torch.nn.Sequential(
            torch.nn.AvgPool1d(kernel_size=5),
            torch.nn.Conv1d(in_channels = 10, out_channels = 10, kernel_size = 6, stride = 1),
            torch.nn.LeakyReLU(),
            torch.nn.BatchNorm1d(num_features=10),
            torch.nn.AvgPool1d(kernel_size=5),
            torch.nn.Conv1d(in_channels=10, out_channels=15, kernel_size=6, stride=2),
            torch.nn.LeakyReLU(),
            torch.nn.BatchNorm1d(num_features=15, eps=0.0001),
        )
        outer_features = ((inner_features//5-5)//5-6)//2+1

        self.concat_features = math.ceil(output_features/2)
        self.dense = torch.nn.Sequential(
            torch.nn.Linear(in_features=outer_features*15, out_features=output_features//2),
            torch.nn.Tanh()
        )

    def forward(self, series):
        series = torch.unsqueeze(series, dim = 1)
        inner = self.conv1(series)
        outer = self.dense(torch.flatten(self.conv2(inner), start_dim=1))
        inner = torch.flatten(inner, start_dim=1)
        return torch.concat([inner[:,-self.concat_features:], outer], dim=1)

class SeriesLayer(torch.nn.Module):
    """40 layers"""
    def __init__(self, output_features: int = 100):
        super().__init__()
        self.individual_layers = torch.nn.ModuleList([
            IndividualSeriesLayer(input_length=example.D1_PRICES if i < 2 else example.H1_PRICES, output_features=output_features)
            for i in range(4)
        ])
    
    def forward(self, series1, series2, series3, series4):
        series = [series1, series2, series3, series4]
        results = [self.individual_layers[i](series[i]) for i in range(4)]
        return torch.concat(results, dim=1)

class Model(torch.nn.Module):
    """64 layers"""
    def __init__(self, individual_text_features: int = 40, final_text_features: int = 100, series_features: int = 100):
        super().__init__()
        self.text_layer = TextLayer(individual_features=individual_text_features, out_features=final_text_features)
        self.series_layer = SeriesLayer(output_features=series_features)

        combined_features = final_text_features + series_features*4
        self.dense1 = torch.nn.Sequential(
            torch.nn.Linear(in_features=combined_features, out_features=100),
            torch.nn.Tanh()
        )
        self.dense2 = torch.nn.Sequential(
            torch.nn.Linear(in_features=100, out_features=10),
            torch.nn.Tanh()
        )
        self.dense3 = torch.nn.Sequential(
            torch.nn.Linear(in_features=10, out_features=1),
            torch.nn.Tanh()
        )
    def forward(self, series1, series2, series3, series4, text1, text2, text3):
        text_out = self.text_layer(text1, text2, text3)
        series_out = self.series_layer(series1, series2, series3, series4)
        combined = torch.concat([series_out, text_out], dim = 1)
        res = self.dense1(combined)
        res = self.dense2(res)
        res = self.dense3(res)
        return res
    
    @staticmethod
    def print_summary():
        model = Model()
        input = [(config.batch_size, example.D1_PRICES)]*2
        input += [(config.batch_size, example.H1_PRICES)]*2
        input += [(config.batch_size, example.TEXT_EMBEDDING_SIZE)]*3
        torchinfo.summary(model, input_size=input)

def extract_tensors(batch: torch.Tensor) -> torch.Tensor:
    series1 = batch[:,example.D1_PRICES_I:example.D1_PRICES_I+example.D1_PRICES]
    series2 = batch[:,example.D1_VOLUMES_I:example.D1_VOLUMES_I+example.D1_PRICES]
    series3 = batch[:,example.H1_PRICES_I:example.H1_PRICES_I+example.H1_PRICES]
    series4 = batch[:,example.H1_VOLUMES_I:example.H1_VOLUMES_I+example.H1_PRICES]
    text1 = batch[:,example.TEXT1_I:example.TEXT1_I+example.TEXT_EMBEDDING_SIZE]
    text2 = batch[:,example.TEXT2_I:example.TEXT2_I+example.TEXT_EMBEDDING_SIZE]
    text3 = batch[:,example.TEXT3_I:example.TEXT3_I+example.TEXT_EMBEDDING_SIZE]
    expect = batch[:,example.D1_TARGET_I]
    expect = PriceTarget.TANH_10_10.get_price(expect)
    return series1, series2, series3, series4, text1, text2, text3, expect
    
"""64 layers * 100 features * 8 bytes ~ 50kB
    50kB * 5000 examples = 250MB

    Let's keep it 1000 examples per batch

"""
