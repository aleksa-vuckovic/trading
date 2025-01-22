import torch
from . import example
import math


class IndividualTextLayer(torch.nn.Module):
    """4 layers"""
    def __init__(self, out_features = 40):
        super().__init__()
        self.dense1 = torch.nn.Linear(in_features = example.TEXT_EMBEDDING_SIZE, out_features=out_features) #40*768=30720
        self.relu1 = torch.nn.GELU()
        self.dense2 = torch.nn.Linear(in_features=out_features, out_features=out_features)
        self.relu2 = torch.nn.GELU()

    def forward(self, input: torch.Tensor):
        return self.relu2(self.dense2(self.tanh(self.dense1(input))))
    
class CombinerLayer(torch.nn.Module):
    """4 layers"""
    def __init__(
        self,
        in_features_left: int,
        in_features_right: int,
        reduced_features: int,
        concat_input: bool = False
    ):
        super().__init__()
        self.left_reducer = torch.nn.Sequential(
            torch.nn.Linear(in_features=in_features_left, out_features=reduced_features),
            torch.nn.GELU()
        )
        self.right_reducer = torch.nn.Sequential(
            torch.nn.Linear(in_features=in_features_right, out_features=reduced_features),
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
    """6 layers"""
    def __init__(self, in_features: int = 40, out_features = 100):
        super().__init__()
        self.combiner = CombinerLayer(
            in_features_left=in_features,
            in_features_right=in_features*2,
            reduced_features=15,
            concat_input=True #225+120 = 345 out features
        )
        self.finalizer = torch.nn.Sequential(
            torch.nn.Linear(in_features=15**2+3*in_features, out_features=out_features),
            torch.nn.GELU()
        )

    def forward(self, text1, text2, text3):
        combined = self.combiner(text1, torch.concat([text2, text3], dim=1))
        return self.finalizer(combined)
    
class TextLayer(torch.nn.Module):
    """18 layers"""
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
    """10 layers"""
    def __init__(self, input_length: int, output_features: int = 50):
        super().__init__()
        features = input_length
        self.conv1 = torch.nn.Sequential(
            torch.nn.Conv1d(in_channels = 1, out_channels = 3, kernel_size = 4, stride = 1),
            torch.nn.LeakyReLU(),
            torch.nn.AvgPool1d(kernel_size=2)
        )
        features = (features-3)//2

        self.conv2 = torch.nn.Sequential(
            torch.nn.Conv1d(in_channels = 3, out_channels = 5, kernel_size = 6, stride = 1),
            torch.nn.LeakyReLU(),
            torch.nn.AvgPool1d(kernel_size=5)
        )
        features = (features-5)//5

        self.conv3 = torch.nn.Sequential(
            torch.nn.Conv1d(in_channels = 5, out_channels = 7, kernel_size = 10, stride = 1),
            torch.nn.LeakyReLU()
        )
        features = features - 9

        self.dense = torch.nn.Sequential(
            torch.nn.Linear(in_features=features*7, out_features=output_features),
            torch.nn.Tanh()
        )

    def forward(self, series):
        series = torch.unsqueeze(series, dim = 1)
        x = self.conv3(self.conv2(self.conv1(series)))
        x = torch.flatten(x, start_dim=1)
        return self.dense(x)

class SeriesLayer(torch.nn.Module):
    """40 layers"""
    def __init__(self, output_features: int = 50):
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
    def __init__(self, individual_text_features: int = 40, final_text_features: int = 100, series_features: int = 50):
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
        return self.dense3(self.dense2(self.dense3(combined)))
    
"""64 layers * 100 features * 8 bytes ~ 50kB
    50kB * 5000 examples = 250MB

    Let's keep it 1000 examples per batch
"""
