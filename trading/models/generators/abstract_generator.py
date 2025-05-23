import logging
import torch
from tqdm import tqdm
from torch import Tensor
from pathlib import Path
from base.algos import binary_search
from base.key_value_storage import FileKVStorage
from base.serialization import serializer
from trading.core import Interval
from trading.core.securities import Exchange, Security, SecurityType
from trading.core.timing_config import TimingConfig
from trading.models.base.batches import BatchFile

logger = logging.getLogger(__name__)

class AbstractGenerator:
    STATE_FILE = '_state.db'

    #region Abstract
    def get_time_frame(self, it: Security|Exchange) -> tuple[float,float]: ...
    def get_folder(self) -> Path: ...
    def get_interval(self) -> Interval: ...
    def get_batch_size(self) -> int: ...
    def generate_example(self, security: Security, end_time: float, with_output: bool = True) -> dict[str, Tensor]: ...
    def plot_statistics(self, **kwargs): ...
    #endregion

    def run(self, exchange: Exchange, timing: TimingConfig):
        folder = self.get_folder()
        time_frame = self.get_time_frame(exchange)
        interval = self.get_interval()
        batch_size = self.get_batch_size()
        securities: list[Security] = [it for it in exchange.securities() if it.type == SecurityType.STOCK]
        securities.sort(key = lambda it: it.symbol)
        security_time_frame = {it:self.get_time_frame(it) for it in securities}
        storage = FileKVStorage(folder/AbstractGenerator.STATE_FILE)

        def key(time: float): return f"{exchange.mic}-{time}"
        def next_time(time: float) -> tuple[float, int]:
            while True:
                time = timing.next(time, interval, exchange)
                symbol: str|None = storage.try_get(key(time))
                if symbol is None: return time, 0
                i = binary_search(securities, symbol, lambda it: it.symbol, side='GT')
                if i < len(securities): return time, i

        time, i = next_time(time_frame[0])
        current: list[dict[str, Tensor]] = []
        
        msg = f"""----Generating examples into {folder}
        Exchange: {exchange.name}
        Timing config: {serializer.serialize(timing, typed=False)}
        Start time: {exchange.calendar.unix_to_datetime(time_frame[0])}
        End time: {exchange.calendar.unix_to_datetime(time_frame[1])}"""
        logger.info(msg)
        print(msg)
        while time < time_frame[1]:
            with tqdm(total=batch_size, desc=f'Generating for {exchange.calendar.unix_to_datetime(time)}', leave=True) as bar:
                while len(current) < batch_size and i < len(securities):
                    security = securities[i]
                    i += 1
                    if security_time_frame[security][0] >= time or security_time_frame[security][1] < time:
                        continue
                    try:
                        current.append(self.generate_example(security, time, with_output=True))
                        logger.info(f"Generated example for {security.symbol} for end time {exchange.calendar.unix_to_datetime(time)}")
                        bar.update(1)
                    except KeyboardInterrupt:
                        raise
                    except:
                        logger.error(f"Failed to generate example for {security.symbol} for {exchange.calendar.unix_to_datetime(time)}", exc_info=True)
            
            if current:
                data = {key:torch.stack([it[key] for it in current], dim=0) for key in current[0].keys()}
                batch_file = BatchFile.get(folder, time, i, exchange).path
                if batch_file.exists(): raise Exception(f"Batch file {batch_file} already exists.")
                torch.save(data, batch_file)
                logger.info(f"Wrote batch for {exchange.calendar.unix_to_datetime(time)}.")
                storage.set(key(time), securities[i-1].symbol)
                current.clear()
            
            if i < len(securities): continue
            time, i = next_time(time)
        logger.info(f"Finished generator execution. Time {exchange.calendar.unix_to_datetime(time)} is bigger than end time {exchange.calendar.unix_to_datetime(time_frame[1])}.")
