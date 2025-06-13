from typing import Any, Literal

class logging:
    root = "logs/prod"

class models:
    batch_size = 1000

class storage:
    location: Literal['folder', 'db', 'mem'] = 'db'
    local_root_path = "storage/prod"
    local_root_path_tmp = "storage/tmp"
    local_db_path = "storage/prod.db"
    local_db_path_tmp = "storage/tmp.db"
    mongo_uri = f"mongodb+srv://XXXXX:XXXXX@XXXXX.mongodb.net/?retryWrites=true&w=majority&appName=trading"
    mongo_db_name = "trading"
    mongo_db_name_tmp = "trading_tmp"

class providers:
    live_time_frame = 5*24*3600.0

class http:
    type loglevel = Literal['none', 'short', 'long']
    response_log: loglevel = 'short'
    request_log: loglevel = 'short'
