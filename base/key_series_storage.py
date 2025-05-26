#4
from collections import defaultdict
import os
from pathlib import Path
from typing import Callable, Generic, Iterable, Sequence, override, TypeVar
from sqlalchemy import Engine, select, delete
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, sessionmaker
from base.algos import binary_search
from base.files import escape_filename, unescape_filename
from base.serialization import Serializer, GenericSerializer, serializer

T = TypeVar('T')

class KeySeriesStorage(Generic[T]):
    def get(self, key: str, start: float, end: float) -> Sequence[T]: ...
    def set(self, key: str, data: Sequence[T]):
        """Upsert data (based on key+timestamp). Ensures no duplicates."""
        raise NotImplementedError()
    def delete(self, key: str, start: float, end: float): ...
    def keys(self) -> Iterable[str]: ...
    
class MemoryKSStorage(KeySeriesStorage[T]):
    """NOT thread safe."""
    data: dict[str, list[T]]
    def __init__(self, timestamp: Callable[[T], float]):
        self.timestamp = timestamp
        self.data = defaultdict(lambda: [])
    
    @override
    def get(self, key: str, start: float, end: float) -> Sequence[T]:
        data = self.data[key]
        if not data: return []
        i = binary_search(data, start, self.timestamp, side='GT')
        j = binary_search(data, end, self.timestamp, side='GT')
        return data[i:j]
    
    @override
    def set(self, key: str, data: Sequence[T]):
        if not data: return
        start = self.timestamp(data[0])
        end = self.timestamp(data[-1])
        existing = self.data[key]
        i = binary_search(existing, start, self.timestamp, side='GE')
        j = binary_search(existing, end, self.timestamp, side='GT')
        existing[i:j] = data

    @override
    def delete(self, key: str, start: float, end: float):
        data = self.data[key]
        if not data: return
        i = binary_search(data, start, self.timestamp, side='GT')
        j = binary_search(data, end, self.timestamp, side='GT')
        del data[i:j]
    
    @override
    def keys(self) -> Iterable[str]:
        return (key for key,value in self.data.items() if value)

class FolderKSStorage(MemoryKSStorage[T]):
    """NOT thread safe."""
    def __init__(self, root: Path, timestamp: Callable[[T], float]):
        super().__init__(timestamp)
        self.root = root

        self.root.mkdir(parents=True, exist_ok=True)
        for key in os.listdir(self.root):
            path = self.root/key
            self.data[unescape_filename(key)] = serializer.deserialize(path.read_text(), list[T])

    def _save(self, key: str):
        path = self.root/escape_filename(key)
        path.write_text(serializer.serialize(self.data[key]))

    @override
    def set(self, key: str, data: Sequence[T]):
        super().set(key, data)
        self._save(key)
    
    @override
    def delete(self, key: str, start: float, end: float):
        super().delete(key, start, end)
        self._save(key)

class SqlKSStorage(KeySeriesStorage[T]):
    """Thread and multiprocess safe."""
    def __init__(self, engine: Engine, table_name: str, timestamp: Callable[[T], float], serializer: Serializer = GenericSerializer()):
        self.engine = engine
        self.timestamp = timestamp
        self.serializer = serializer
        self.maker = sessionmaker(bind=self.engine)

        Base = declarative_base()
        class Table(Base):
            __tablename__ = table_name
            key: Mapped[str] = mapped_column(primary_key=True)
            timestamp: Mapped[float] = mapped_column(primary_key=True)
            value: Mapped[str]
        self.Table = Table
        Base.metadata.create_all(self.engine)
    
    @override
    def get(self, key: str, start: float, end: float) -> Sequence[T]:
        with self.maker.begin() as sess:
            result = sess.execute(select(self.Table.value).where(
                (self.Table.key == key) & (self.Table.timestamp > start) & (self.Table.timestamp <= end)
            )).scalars().all()
        return [self.serializer.deserialize(it) for it in result]
    
    @override
    def set(self, key: str, data: Sequence):
        toinsert = [self.Table(key=key, timestamp=self.timestamp(it), value=self.serializer.serialize(it)) for it in data]
        with self.maker.begin() as sess:
            for it in toinsert:
                sess.merge(it)

    @override
    def delete(self, key: str, start: float, end: float):
        with self.maker.begin() as sess:
            sess.execute(delete(self.Table).where(
                (self.Table.key == key) & (self.Table.timestamp > start) & (self.Table.timestamp <= end)
            ))

    @override
    def keys(self) -> Iterable[str]:
        with self.maker.begin() as sess:
            return sess.execute(select(self.Table.key).distinct()).scalars().all()
