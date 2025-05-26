#2
from typing import override
from logging import Formatter
from base import dates
import traceback
import json

class JsonFormatter(Formatter):
    def __init__(self):
        pass
    @override
    def format(self, record):
        log_message = {
            "time": dates.unix_to_str(record.created, format="%Y-%m-%d %H:%M:%S.%f", tz=dates.UTC),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
            "exception": traceback.format_exception(record.exc_info[1]) if record.exc_info else None
        }
        return json.dumps(log_message)

text_format = Formatter('%(asctime)s \t- %(name)s - %(levelname)s - %(message)s')
json_format = JsonFormatter()