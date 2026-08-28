"""Rough ingest-throughput and query-latency benchmark for stored.

Run: ``uv run python bench/ingest.py``. Not a test — a quick, dependency-light
sanity check on the batched writer and range queries against DuckDB.
"""
from __future__ import annotations

import tempfile
import time
from pathlib import Path

import seared as s

import stored


@s.seared
class Sample(s.Seared):
    id: int = s.Int(required=True)
    x: float = s.Float(default=0.0)


def bench(n: int) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = stored.Store(str(Path(tmp) / "bench.duckdb"), flush_secs=0)
        try:
            store.register(Sample, index=("id",))

            start = time.perf_counter()
            for i in range(n):
                store.record(Sample, Sample(id=i % 1000, x=float(i)), key=f"bench/{i % 1000}")
            store.flush()
            ingest = time.perf_counter() - start

            start = time.perf_counter()
            rows = store.query(Sample, limit=n)
            query = time.perf_counter() - start
        finally:
            store.close()

    print(f"ingest: {n} rows in {ingest:.3f}s ({n / ingest:,.0f} rows/s)")
    print(f"query:  {len(rows)} rows in {query * 1000:.1f}ms")


if __name__ == "__main__":
    bench(25_000)
