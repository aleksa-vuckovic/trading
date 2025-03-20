from pathlib import Path
from typing import Literal


class models:
    batch_size = 1000

class caching:
    storage: Literal['file','db','none']='file'
    file_path: Path = Path('./cache/test')
    db_path: Path = Path('./cache/test.db')

class http:
    log_response=False
    log_response_short=True
    log_request=False
    log_request_short=True