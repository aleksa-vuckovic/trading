from datetime import datetime
from ..utils import dateutils, httputils, common
import logging
from bs4 import BeautifulSoup
from pathlib import Path
import math
import re
import json
import time

logger = logging.getLogger(__name__)
_MODULE: str = __name__.split(".")[-1]
_CACHE: Path = common.CACHE / _MODULE

def _format_date(unix: int) -> str:
    return dateutils.unix_to_datetime(unix, tz = dateutils.EST)\
        .strftime('%b-%d-%Y')\
        .replace("-0", "-")\
        .replace("jun", "june")\
        .lower()

@common.backup_timeout()
def _get_summary(unix_time: int) -> str:
    #First find the id by searching through the pages
    #One page = approximately 1 month (more than 1 month, less than 2 months)
    day_diff = time.time()/(24*3600) - unix_time/(24*3600)
    start_page = 1+int(day_diff/45)
    end_page = min(1+math.ceil(day_diff/30), 90)
    dates = [_format_date(unix_time - i*24*3600) for i in range(5)]
    for i in range(start_page, end_page+1):
        url = f"https://www.zacks.com/blog/archive.php?page={i}&type=json&g=59"
        resp = httputils.get_as_browser(url)
        for date in dates:
            ids = re.findall(r"\\/stock\\/news\\/(\d+)\\/stock-market-news-for-" + date, resp.text)
            if ids:
                id = int(ids[0])
                pageurl = f"https://www.zacks.com/stock/news/{id}/stock-market-news-for-{date}"
                resp = httputils.get_as_browser(pageurl)
                soup = BeautifulSoup(resp.text, 'html.parser')
                main_div = soup.find('div', id='comtext')
                if not main_div:
                    raise ValueError(f"No comtext div. Url: {pageurl}\n Document:\n{resp.text}")
                return "\n".join([it.text for it in main_div.find_all("p", recursive=False)])
    logger.fatal(f"Couldn't find date {dates[0]} in pages from {start_page} to {end_page}.")
    return ""

def get_summary(unix_time: int) -> str:
    path = _CACHE
    path.mkdir(parents = True, exist_ok=True)
    date = _format_date(unix_time)
    path /= date
    if path.exists():
        return path.read_text()
    data = _get_summary(unix_time)
    path.write_text(data)
    return data