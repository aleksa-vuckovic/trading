from pathlib import Path
from typing import Literal

class logging:
    root: Path = Path(__file__).parent/'logs'/'test'

class models:
    batch_size = 1000

class caching:
    storage: Literal['file','db','none']='file'
    file_path: Path = Path(__file__).parent/'cache'/'test'
    db_path: Path = Path(__file__).parent/'cache'/'test.db'

class providers:
    live_time_frame: float = 5*24*3600.0

class http:
    log_response=False
    log_response_short=True
    log_request=False
    log_request_short=True