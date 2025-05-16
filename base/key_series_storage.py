from collections import defaultdict
from typing import Callable, Generic, Iterable, Sequence, final, override, TypeVar

from sqlalchemy import Engine, select, delete
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, sessionmaker

from base.algos import binary_search, binsert
from base.caching import Serializer, TypedSerializer

T = TypeVar('T')
class KeySeriesStorage(Generic[T]):
    @final
    def set(self, key: str, start: float, end: float, data: Sequence[T]):
        spans = list(self._get_overlapping_spans(key, start, end))
        for span in spans: self._delete_span(key, span[0])
        self._delete(key, start, end) #?
        if spans and spans[0][0] < start: start = spans[0][0]
        if spans and spans[-1][1] > end: end = spans[-1][1]
        self._set(key, data)
        self._add_span(key, start, end)
    @final
    def get(self, key: str, start: float, end: float) -> Sequence[T]:
        return self._get(key, start, end)
    @final
    def delete(self, key: str, start: float, end: float):
        spans = list(self._get_overlapping_spans(key, start, end))
        for span in spans: self._delete_span(key, span[0])
        if spans and spans[0][0] < start: self._add_span(key, spans[0][0], start)
        if spans and spans[-1][1] > end: self._add_span(key, end, spans[-1][1])
        self._delete(key, start, end)
    @final
    def keys(self) -> Iterable[str]:
        return self._keys()
    

    def _set(self, key: str, data: Sequence[T]):# -> Any:
        """Set data series. No span updates."""
        raise NotImplementedError()
    def _get(self, key: str, start: float, end: float) -> Sequence[T]:
        """Get data series."""
        raise NotImplementedError()
    def _delete(self, key: str, start: float, end: float):
        """Delete data series. No span updates."""
        raise NotImplementedError()
    def _keys(self) -> Iterable[str]:
        raise NotImplementedError()
    
    def _add_span(self, key: str, start: float, end: float):
        """Add span (start, end). (No overlap checks)."""
        raise NotImplementedError()
    def _get_overlapping_spans(self, key: str, start: float, end: float) -> Iterable[tuple[float, float]]:
        """Get overlapping spans. Adjacent spans are also considered overlapping."""
        raise NotImplementedError()
    def _delete_span(self, key: str, start: float):
        """Delete span starting at start."""
        raise NotImplementedError()
    
    @final
    def missing_spans(self, key: str, start: float, end: float) -> Iterable[tuple[float, float]]:
        spans = list(self._get_overlapping_spans(key, start, end))
        if not spans:
            yield (start, end)
            return
        for i in range(len(spans)+1):
            if i == 0:
                if spans[0][0] > start: yield (start, spans[0][0])
            elif i < len(spans):
                yield (spans[i-1][1], spans[i][0])
            else:
                if spans[-1][1] < end: yield (spans[-1][1], end)
    
class MemoryKeySeriesStorage(KeySeriesStorage[T]):
    data: dict[str, list[T]]
    spans: dict[str, list[tuple[float,float]]]
    def __init__(self, timestamp: Callable[[T], float]):
        self.timestamp = timestamp
        self.data = defaultdict(lambda: [])
        self.spans = defaultdict(lambda: [])
    @override
    def _set(self, key: str, data: Sequence[T]):
        if not data: return
        start = self.timestamp(data[0])
        end = self.timestamp(data[-1])
        existing = self.data[key]
        i = binary_search(existing, start, self.timestamp, side='GE')
        j = binary_search(existing, end, self.timestamp, side='GT')
        existing[i:j] = data
    @override
    def _get(self, key: str, start: float, end: float) -> Sequence[T]:
        data = self.data[key]
        if not data: return []
        i = binary_search(data, start, self.timestamp, side='GT')
        j = binary_search(data, end, self.timestamp, side='GT')
        return data[i:j]
    @override
    def _delete(self, key: str, start: float, end: float):
        data = self.data[key]
        if not data: return
        i = binary_search(data, start, self.timestamp, side='GT')
        j = binary_search(data, end, self.timestamp, side='GT')
        del data[i:j]
    @override
    def _keys(self) -> Iterable[str]:
        return (key for key,value in self.data.items() if value)
    
    @override
    def _add_span(self, key: str, start: float, end: float):
        binsert(self.spans[key], (start, end), key=lambda it: it[0])
    @override
    def _get_overlapping_spans(self, key: str, start: float, end: float) -> list[tuple[float,float]]:
        spans = self.spans[key]
        if not spans: return []
        i = binary_search(spans, start, key=lambda it: it[0], side='LT')    
        j = binary_search(spans, end, key=lambda it: it[1], side='GT')
        if i < 0 or spans[i][1] < start: i += 1
        if j == len(spans) or spans[j][0] > end: j -= 1
        return spans[i:j+1]
    @override
    def _delete_span(self, key: str, start: float):
        spans = self.spans[key]
        i = binary_search(spans, start, key=lambda it: it[0], side='EQ')
        if i is not None: spans.pop(i)

class SqlKeySeriesStorage(KeySeriesStorage[T]):
    def __init__(self, engine: Engine, table_name: str, timestamp: Callable[[T], float], serializer: Serializer = TypedSerializer()):
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
        class Span(Base):
            __tablename__ = f"{table_name}_span"
            key: Mapped[str] = mapped_column(primary_key=True)
            start: Mapped[float] = mapped_column(primary_key=True)
            end: Mapped[float]
        self.Span = Span
        Base.metadata.create_all(self.engine)
    
    @override
    def _set(self, key: str, data: Sequence):
        toinsert = [self.Table(key=key, timestamp=self.timestamp(it), value=self.serializer.serialize(it)) for it in data]
        with self.maker.begin() as sess:
            sess.add_all(toinsert)
    @override
    def _get(self, key: str, start: float, end: float) -> Sequence[T]:
        with self.maker.begin() as sess:
            result = sess.execute(select(self.Table.value).where(
                (self.Table.key == key) & (self.Table.timestamp > start) & (self.Table.timestamp <= end)
            )).scalars().all()
        return [self.serializer.deserialize(it) for it in result]
    @override
    def _delete(self, key: str, start: float, end: float):
        with self.maker.begin() as sess:
            sess.execute(delete(self.Table).where(
                (self.Table.key == key) & (self.Table.timestamp > start) & (self.Table.timestamp <= end)
            ))
    @override
    def _keys(self) -> Iterable[str]:
        with self.maker.begin() as sess:
            return sess.execute(select(self.Table.key).distinct()).scalars().all()
    
    @override
    def _add_span(self, key: str, start: float, end: float):
        with self.maker.begin() as sess:
            sess.add(self.Span(key=key, start=start, end=end))
    @override
    def _get_overlapping_spans(self, key: str, start: float, end: float) -> Iterable[tuple[float, float]]:
        with self.maker.begin() as sess:
            result = sess.execute(select(self.Span).where(
                ((self.Span.start >= start) & (self.Span.start <= end)) |
                ((self.Span.start < start) & (self.Span.end >= start))
            )).scalars().all()
            sess.expunge_all()
        return [(it.start, it.end) for it in result]
    @override
    def _delete_span(self, key: str, start: float):
        with self.maker.begin() as sess:
            sess.execute(delete(self.Span).where((self.Span.key == key) & (self.Span.start == start)))

    
