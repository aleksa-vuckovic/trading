from .network import Model
import os
from pathlib import Path
import re
import torch
from torch import optim
from tqdm import tqdm
from . import example
import logging

logger = logging.getLogger(__name__)
#Using 1/6th for validation
examples_folder = Path(__file__).parent / 'examples'
checkpoint_file = Path(__file__).parent / 'checkpoint.pth'
special_checkpoint_file = Path(__file__).parent / 'special_checkpoint.pth'
learning_rate = 10e-6


def get_all_files() -> list[dict]:
    pattern = re.compile(r"([^_]+)_batch(\d+)-(\d+).pt")
    files = [ pattern.fullmatch(it) for it in os.listdir(examples_folder)]
    files = [ {'file': it.group(0), 'source': it.group(1), 'batch': int(it.group(2)), 'hour': int(it.group(3))} for it in files if it ]
    return sorted(files, key=lambda it: (it['source'], it['hour'], it['batch']))
all_files = get_all_files()

def get_training_files() -> list[str]:
    return [it for it in all_files if it['batch'] % 6]

def get_validation_files() -> list[str]:
    return [it for it in all_files if it['batch']%6 == 0]

def run_loop(max_epochs = 100000000):
    training_files = get_training_files()
    validation_files = get_validation_files()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info(f"Loaded {len(training_files)} training and {len(validation_files)} validation batches.")
    logger.info(f"Device: {device}")
    model = Model().to(device)
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)
    #loss_fn = torch.nn.MSELoss()
    def loss_fn(x, y):
        eps = 1e-5
        loss = -torch.log(1 + eps - torch.abs(x - y) / (1+torch.abs(y)))
        return loss.mean()
    def accuracy_precision_fn(output: torch.Tensor, expect: torch.Tensor) -> float:
        #Take 0.5 as the breaking point, and asses how many of these are recognized
        output = output > 0.5
        expect = expect > 0.5
        hits = torch.logical_and(output, expect).sum().item()
        output_n = output.sum().item()
        expect_n = expect.sum().item()
        return hits / output_n if output_n else 0 if expect_n else 1, hits / expect_n if expect_n else 1

    if checkpoint_file.exists():
        data = torch.load(checkpoint_file, weights_only=True, map_location=device)
        model.load_state_dict(data['model_state_dict'])
        optimizer.load_state_dict(data['optimizer_state_dict'])
        epoch = data['epoch'] + 1
        history = data['history']
        logger.info(f"Restored state from epoch {epoch-1}")
    else:
        logger.info(f"No state, starting from scratch.")
        epoch = 1
        history = []

    def extract_tensors(batch: torch.Tensor) -> torch.Tensor:
        series1 = batch[:,example.D1_PRICES_I:example.D1_PRICES_I+example.D1_PRICES]
        series2 = batch[:,example.D1_VOLUMES_I:example.D1_VOLUMES_I+example.D1_PRICES]
        series3 = batch[:,example.H1_PRICES_I:example.H1_PRICES_I+example.H1_PRICES]
        series4 = batch[:,example.H1_VOLUMES_I:example.H1_VOLUMES_I+example.H1_PRICES]
        text1 = batch[:,example.TEXT1_I:example.TEXT1_I+example.TEXT_EMBEDDING_SIZE]
        text2 = batch[:,example.TEXT2_I:example.TEXT2_I+example.TEXT_EMBEDDING_SIZE]
        text3 = batch[:,example.TEXT3_I:example.TEXT3_I+example.TEXT_EMBEDDING_SIZE]
        expect = batch[:,example.D1_TARGET_I]
        expect = example.PriceTarget.TANH_10_10.get_price(expect)
        return series1, series2, series3, series4, text1, text2, text3, expect
    
    while epoch < max_epochs:
        model.train()
        total_train_loss = 0
        total_train_accuracy = 0
        total_train_precision = 0
        total_val_loss = 0
        total_val_accuracy = 0
        total_val_precision = 0
        with tqdm(training_files, desc=f"Epoch {epoch}", leave=True) as bar:
            for item in bar:
                batch = torch.load(examples_folder / item['file'], weights_only=True).to(device, dtype=torch.float32)
                logger.info(f"Loaded a batch of shape {batch.shape}")
                tensors = extract_tensors(batch)
                input = tensors[:-1]
                expect = tensors[-1]

                optimizer.zero_grad()
                output = model(*input).squeeze()
                loss = loss_fn(output, expect)
                accuracy, precision = accuracy_precision_fn(output, expect)
                loss.backward()
                optimizer.step()
                
                total_train_loss += loss.item()
                total_train_accuracy += accuracy
                total_train_precision += precision
                n = bar.n+1
                bar.set_postfix(loss = loss.item(), accuracy = accuracy, precision = precision, total_loss = total_train_loss/n, total_accuracy = total_train_accuracy/n, total_precision = total_train_precision/n)
        n = len(training_files)
        total_train_loss /= n
        total_train_accuracy /= n
        total_train_precision /= n
        
        model.eval()
        with torch.no_grad():
            with tqdm(validation_files, desc = 'Validation...', leave=False) as bar:
                for item in bar:
                    batch = torch.load(examples_folder / item['file'], weights_only=True).to(device, dtype=torch.float32)
                    tensors = extract_tensors(batch)
                    input = tensors[:-1]
                    expect = tensors[-1]

                    output = model(*input).squeeze()
                    loss = loss_fn(output, expect)
                    accuracy, precision = accuracy_precision_fn(output, expect)

                    total_val_loss += loss.item()
                    total_val_accuracy += accuracy
                    total_val_precision += precision
                    
        n = len(validation_files)
        total_val_loss /= n
        total_val_accuracy /= n
        total_val_precision /= n
        print(f"Validation: loss={total_val_loss:.4f} \taccuracy={total_val_accuracy:.4f} \tprecision={total_val_precision:.4f}")
        history.append({
            'total_train_loss': total_train_loss,
            'total_train_accuracy': total_train_accuracy,
            'total_train_precision': total_train_precision,
            'total_val_loss': total_val_loss,
            'total_val_accuracy': total_val_accuracy,
            'total_val_precision': total_val_precision
        })
        epoch += 1

        savedict = {
            'model_state_dict': model.state_dict(),
            'optimizer_state_dict': optimizer.state_dict(),
            'epoch': epoch,
            'history': history
        }
        torch.save(savedict, checkpoint_file)

            



                
                
