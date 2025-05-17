from pathlib import Path
from typing import Literal

class logging:
    root: Path = Path(__file__).parent/'logs'/'prod'

class models:
    batch_size = 1000

class storage:
    type loc = Literal['folder', 'db', 'mem']
    location: loc = 'db'
    folder_path: Path = Path(__file__).parent/'storage'/'prod'
    db_path: Path = Path(__file__).parent/'storage'/'prod.db'

class providers:
    live_time_frame: float = 5*24*3600.0

class http:
    type loglevel = Literal['none', 'short', 'long']
    response_log: loglevel = 'short'
    request_log: loglevel = 'short'