from pymongo import MongoClient
from sqlalchemy import create_engine
import config
import dns.resolver
dns.resolver.get_default_resolver().nameservers = ["8.8.8.8"]

local_db = create_engine(f"sqlite:///{config.storage.local_db_path}")
local_db_tmp = create_engine(f"sqlite:///{config.storage.local_db_path_tmp}")

mongo_client = MongoClient(config.storage.mongo_uri)
mongo_db = mongo_client["trading"]
mongo_db_tmp = mongo_client["trading_tmp"]
