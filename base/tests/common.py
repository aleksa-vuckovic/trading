from typing import Any, Literal, override
from unittest import TestCase
from pathlib import Path
import shutil
import uuid
from sqlalchemy import Engine
from sqlalchemy.sql.schema import HasSchemaAttr
from base import dates
from base.algos import random_b32
from base.caching import KeySeriesStorage, KeyValueStorage
from base.key_series_storage import FolderKSStorage, MemoryKSStorage, SqlKSStorage
from base.key_value_storage import FileKVStorage, FolderKVStorage, MemoryKVStorage, SqlKVStorage
from base.types import Equatable, Serializable
from base.db import sqlite_engine

TEST_ROOT = Path("./test_data")

class A(Equatable, Serializable):
    def __init__(self, t: float, d: Any = None):
        self.t = float(t)
        self.d = d
    def __repr__(self) -> str: return f"A(t={self.t},d={self.d})"

type storage_type = Literal['mem', 'folder', 'file', 'sqlite']
kv_types = ['mem', 'folder', 'file', 'sqlite']
ks_types = ['mem', 'folder', 'sqlite']

class TestPersistence(TestCase):
    sqlite_engine: Engine
    def get_kv_storage(self, storage_type: storage_type) -> KeyValueStorage:
        if storage_type == 'mem': return MemoryKVStorage()
        if storage_type == 'folder': return FolderKVStorage(TEST_ROOT/random_b32())
        if storage_type == 'file': return FileKVStorage(TEST_ROOT/random_b32())
        if storage_type == 'sqlite': return SqlKVStorage(self.sqlite_engine, random_b32())
        raise Exception(f"Unsupported kv storage type {storage_type}.")
    def get_ks_storage(self, type: storage_type) -> KeySeriesStorage:
        if type == 'mem': return MemoryKSStorage[A](lambda it: it.t)
        if type == 'folder': return FolderKSStorage[A](TEST_ROOT/random_b32(), lambda it: it.t)
        if type == 'sqlite': return SqlKSStorage[A](self.sqlite_engine, 'test', lambda it: it.t)
        raise Exception(f"Unsupported ks storage type {type}.")
    @override
    def setUp(self):
        super().setUp()
        if TEST_ROOT.exists(): shutil.rmtree(TEST_ROOT)
        TEST_ROOT.mkdir(parents=True, exist_ok=True)
        self.sqlite_engine = sqlite_engine(TEST_ROOT/random_b32())
        dates.set(None)
    @override
    def tearDown(self):
        super().tearDown()
        if hasattr(self, 'sqlite_engine'):
            self.sqlite_engine.dispose()
            del self.sqlite_engine
        if TEST_ROOT.exists(): shutil.rmtree(TEST_ROOT)
        dates.set(None)
