"""Destinos de los registros: Kinesis (PutRecords), archivo JSONL o nulo (solo cuenta)."""
from __future__ import annotations

import json
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterable

from .events import Record

MAX_RECORDS = 500            # límite de PutRecords
MAX_BYTES = 5 * 1024 * 1024  # límite de PutRecords (5 MiB por llamada)


class Sink:
    def send(self, records: list[Record]) -> None: ...
    def close(self) -> dict:
        return {}


class NullSink(Sink):
    def __init__(self):
        self.count = 0

    def send(self, records):
        self.count += len(records)

    def close(self):
        return {"sent": self.count}


class FileSink(Sink):
    """Un archivo JSONL por stream lógico. Los JSON truncados se guardan tal cual en `raw`."""

    def __init__(self, directory: str | Path):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self._fh: dict[str, object] = {}
        self.count = 0

    def send(self, records):
        for r in records:
            fh = self._fh.get(r.stream_type)
            if fh is None:
                fh = self._fh[r.stream_type] = (self.dir / f"{r.stream_type}.jsonl").open("ab")
            fh.write(r.payload() + b"\n")  # type: ignore[union-attr]
            self.count += 1

    def close(self):
        for fh in self._fh.values():
            fh.close()  # type: ignore[union-attr]
        return {"sent": self.count, "dir": str(self.dir)}


def chunk_records(records: Iterable[Record]) -> Iterable[list[Record]]:
    """Agrupa respetando los límites de PutRecords (500 registros y 5 MiB por llamada)."""
    chunk, size = [], 0
    for r in records:
        n = len(r.payload()) + len(r.key.encode())
        if chunk and (len(chunk) >= MAX_RECORDS or size + n > MAX_BYTES):
            yield chunk
            chunk, size = [], 0
        chunk.append(r)
        size += n
    if chunk:
        yield chunk


class KinesisSink(Sink):
    """PutRecords concurrente con reintento de los registros que Kinesis rechaza.

    `stream_names` mapea el tipo lógico al stream real. Con un único stream compartido
    (diseño del diagrama) basta pasar el mismo nombre para los tres tipos.
    """

    def __init__(self, stream_names: dict[str, str], region: str = "us-east-1", workers: int = 16,
                 max_attempts: int = 8, client=None):
        if client is None:
            import boto3  # diferido
            from botocore.config import Config
            client = boto3.client("kinesis", region_name=region,
                                  config=Config(max_pool_connections=workers * 2, retries={"max_attempts": 3, "mode": "standard"}))
        self.client, self.names, self.max_attempts = client, stream_names, max_attempts
        self._pool = ThreadPoolExecutor(max_workers=workers)
        self._slots = threading.Semaphore(workers * 4)  # contrapresión: limita llamadas en vuelo
        self._lock = threading.Lock()
        self._futures: list = []
        self.stats = Counter()

    def send(self, records):
        by_stream: dict[str, list[Record]] = {}
        for r in records:
            by_stream.setdefault(self.names[r.stream_type], []).append(r)
        for stream, recs in by_stream.items():
            for chunk in chunk_records(recs):
                self._slots.acquire()
                self._futures.append(self._pool.submit(self._put, stream, chunk))
        self._futures = [f for f in self._futures if not f.done() or f.exception()]  # conserva fallos

    def _put(self, stream: str, chunk: list[Record]) -> None:
        try:
            pending = chunk
            for attempt in range(self.max_attempts):
                resp = self.client.put_records(
                    StreamName=stream, Records=[{"Data": r.payload(), "PartitionKey": r.key} for r in pending])
                failed = resp.get("FailedRecordCount", 0)
                with self._lock:
                    self.stats["ok"] += len(pending) - failed
                if not failed:
                    return
                pending = [r for r, res in zip(pending, resp["Records"]) if "ErrorCode" in res]
                with self._lock:
                    self.stats["throttled_retries"] += len(pending)
                time.sleep(min(0.05 * 2 ** attempt, 2.0))
            with self._lock:
                self.stats["lost"] += len(pending)
        finally:
            self._slots.release()

    def close(self):
        self._pool.shutdown(wait=True)
        errs = [f.exception() for f in self._futures if f.exception()]
        out = dict(self.stats)
        if errs:
            out["errors"] = [repr(e) for e in errs[:3]]
        return out


def read_jsonl_records(path: str | Path) -> list[dict]:
    """Utilidad de pruebas: lee un JSONL ignorando líneas corruptas."""
    out = []
    for line in Path(path).read_bytes().splitlines():
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return out
