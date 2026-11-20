#!/usr/bin/env python3
"""
Data sources for the OTel-vs-Kafka resiliency demo (stdlib only).

Each SOURCE is paired 1:1 with an AGENT collector and pushes OTLP/HTTP (JSON) logs (+ a metric every 5 s).
Every log record carries a unique, deterministic event_id = uuid5(source, seq) and a monotonic seq.

GROUND TRUTH LEDGER (SQLite, one file per source, on a host bind mount):
    events(seq, event_id, gen_ts_ms, acked, attempts, ack_ts_ms)
    acked = 1  <=>  the agent answered 2xx (the source was told "I have it")
verify.py later compares the ledger with what actually arrived in Elasticsearch:
    acked but missing in ES  => SILENT DATA LOSS (the source will never resend it)

The source behaves like an application with a bounded in-memory buffer:
    producer thread  : generates events at a fixed rate into a bounded queue (never blocks, overflow counted as 'source_dropped')
    sender thread    : POSTs batches to the agent; with CLIENT_RETRY=on it retries the SAME batch (same event_ids) until
                       acked or CLIENT_RETRY_SECONDS expires (=> duplicates are possible, which is the at-least-once trade-off)
"""

import json
import os
import queue
import random
import string
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

from typing import Any, Callable, Self

NS: uuid.UUID = uuid.UUID(hex="6f1c2b0e-7d7a-4b1c-9a55-0c0ffee0d0d0")

_ALNUM = string.ascii_letters + string.digits


def random_pad(n: int = 200) -> str:
    """Random alphanumeric filler: realistic record size, and it does not compress unrealistically well."""
    return "".join(random.choices(_ALNUM, k=n))


def now_ms() -> int:
    return int(time.time() * 1000)


def attr(k, v) -> dict[str, Any]:
    if isinstance(v, int):
        return {
            "key": k,
            "value": {
                "intValue": str(v),
            },
        }
    return {
        "key": k,
        "value": {
            "stringValue": str(v),
        },
    }


class Source(threading.Thread):
    def __init__(
        self,
        name,
        url,
        rate,
        batch,
        seed,
        duration,
        retry_on,
        retry_seconds,
        drain_seconds,
        ledger_dir,
        senders=8,
        qmax=500_000,
    ) -> None:
        super().__init__(daemon=False, name=name)
        self.src, self.url, self.rate, self.batch = name, url, max(rate, 1.0), batch
        self.duration, self.retry_on = duration, retry_on
        self.retry_seconds, self.drain_seconds = retry_seconds, drain_seconds
        self.q = queue.Queue(maxsize=qmax)
        self.stats = dict(
            generated=0,
            source_dropped=0,
            acked=0,
            failed_final=0,
            http_attempts=0,
            http_errors=0,
        )
        self.path = os.path.join(ledger_dir, f"ledger-{name}.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(path=self.path + suffix)
            except FileNotFoundError:
                pass
        self.db = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("""
            CREATE TABLE events(
                seq INTEGER PRIMARY KEY,
                event_id TEXT,
                gen_ts_ms INTEGER, 
                acked INTEGER DEFAULT 0,
                attempts INTEGER DEFAULT 0,
                ack_ts_ms INTEGER
            )""")
        self.senders = max(1, senders)
        self.lock = threading.Lock()
        self.slock = threading.Lock()  # protects the counters (several sender threads)
        self.gen_done = threading.Event()

    # ---------------------------------------------------------------- producer
    def producer(self) -> None:
        seq = 0
        t0: float = time.time()
        carry: float = 0.0
        while time.time() - t0 < self.duration:
            tick: float = time.time()
            carry += (
                self.rate * 0.1
            )  # generate in 100 ms slices (carry keeps fractional rates exact)
            n: int = int(carry)
            carry -= n
            rows, evs = [], []
            for _ in range(n):
                seq += 1
                eid: str = str(uuid.uuid5(namespace=NS, name=f"{self.src}:{seq}"))
                ts: int = now_ms()
                evs.append((seq, eid, ts))
                rows.append((seq, eid, ts))
            with self.lock:
                self.db.execute("BEGIN")
                self.db.executemany(
                    "INSERT INTO events(seq,event_id,gen_ts_ms) VALUES(?,?,?)",
                    rows,
                )
                self.db.execute("COMMIT")
            self.stats["generated"] += n
            for i in range(0, len(evs), self.batch):   # one queue item = one request worth of events
                chunk = evs[i : i + self.batch]
                try:
                    self.q.put_nowait(item=chunk)
                except queue.Full:
                    self.stats["source_dropped"] += len(chunk)
            time.sleep(max(0.0, 0.1 - (time.time() - tick)))
        self.gen_done.set()

    # ---------------------------------------------------------------- OTLP payloads
    def logs_payload(self, evs) -> dict[str, list[dict[str, Any]]]:
        recs: list[Any] = []
        for seq, eid, ts in evs:
            # structured (map) body: the Kafka exporter's "raw" encoding then emits it as a plain JSON object
            body: dict[str, dict[str, list[dict[str, Any]]]] = {
                "kvlistValue": {
                    "values": [
                        attr(k="event_id", v=eid),
                        attr(k="seq", v=seq),
                        attr(k="source_id", v=self.src),
                        attr(k="gen_ts_ms", v=ts),
                        attr(k="msg", v="demo log line"),
                        attr(k="pad", v=random_pad()),
                    ]
                }
            }
            recs.append(
                {
                    "timeUnixNano": str(ts * 1_000_000),
                    "severityText": "INFO",
                    "severityNumber": 9,
                    "body": body,
                    "attributes": [
                        attr(k="event_id", v=eid),
                        attr(k="seq", v=seq),
                        attr(k="source_id", v=self.src),
                        attr(k="elasticsearch.document_id", v=eid),
                    ],
                }
            )
        return {
            "resourceLogs": [
                {
                    "resource": {
                        "attributes": [attr("service.name", self.src)],
                    },
                    "scopeLogs": [
                        {
                            "scope": {"name": "demo-generator"},
                            "logRecords": recs,
                        }
                    ],
                }
            ]
        }

    def metrics_payload(self) -> dict[str, list[dict[str, Any]]]:
        t: str = str(now_ms() * 1_000_000)
        return {
            "resourceMetrics": [
                {
                    "resource": {
                        "attributes": [
                            attr(
                                k="service.name",
                                v=self.src,
                            )
                        ],
                    },
                    "scopeMetrics": [
                        {
                            "scope": {
                                "name": "demo-generator",
                            },
                            "metrics": [
                                {
                                    "name": "demo.events.generated",
                                    "unit": "1",
                                    "sum": {
                                        "aggregationTemporality": 2,
                                        "isMonotonic": True,
                                        "dataPoints": [
                                            {
                                                "asInt": str(self.stats["generated"]),
                                                "startTimeUnixNano": t,
                                                "timeUnixNano": t,
                                            }
                                        ],
                                    },
                                }
                            ],
                        }
                    ],
                }
            ]
        }

    def post(self, path, payload, timeout=10) -> bool:
        req: urllib.request.Request = urllib.request.Request(
            url=self.url + path,
            data=json.dumps(obj=payload).encode(),
            headers={
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with self.slock:
            self.stats["http_attempts"] += 1
        try:
            with urllib.request.urlopen(url=req, timeout=timeout) as r:
                body = r.read()
                if not (200 <= r.status < 300):
                    return False
                try:  # OTLP partial success => not everything was accepted
                    ps: Any | dict[Any, Any] = (json.loads(s=body or b"{}") or {}).get(
                        "partialSuccess"
                    ) or {}
                    if int(ps.get("rejectedLogRecords", 0) or 0) > 0:
                        return False
                except ValueError:
                    pass
                return True
        except (urllib.error.URLError, OSError, ValueError):
            with self.slock:
                self.stats["http_errors"] += 1
            return False

    # ---------------------------------------------------------------- sender
    def mark(self, evs, acked) -> None:
        ts: int = now_ms()
        with self.lock:
            self.db.execute("BEGIN")
            if acked:
                self.db.executemany(
                    "UPDATE events SET acked=1, attempts=attempts+1, ack_ts_ms=? WHERE seq=?",
                    [(ts, e[0]) for e in evs],
                )
            else:
                self.db.executemany(
                    "UPDATE events SET attempts=attempts+1 WHERE seq=?",
                    [(e[0],) for e in evs],
                )
            self.db.execute("COMMIT")

    def sender(self, idx=0) -> None:
        last_metric = 0
        drain_deadline = None
        while True:
            if self.gen_done.is_set():
                drain_deadline = drain_deadline or time.time() + self.drain_seconds
                if self.q.empty() or time.time() > drain_deadline:
                    break
            try:
                evs: list[Any] = self.q.get(timeout=0.2)
            except queue.Empty:
                continue
            payload: dict[str, list[dict[str, Any]]] = self.logs_payload(evs)
            deadline: float = time.time() + self.retry_seconds
            backoff: float = 0.5
            while True:
                ok = self.post("/v1/logs", payload)
                if ok:
                    self.mark(evs, acked=True)
                    with self.slock:
                        self.stats["acked"] += len(evs)
                    break
                self.mark(evs, acked=False)
                if not self.retry_on or time.time() > deadline:
                    with self.slock:
                        self.stats["failed_final"] += len(
                            evs
                        )  # the source gives up: it KNOWS this data was not accepted
                    break
                time.sleep(backoff)
                backoff: float = min(backoff * 2, 5.0)
            if idx == 0 and time.time() - last_metric > 5:
                last_metric: float = time.time()
                self.post(
                    path="/v1/metrics",
                    payload=self.metrics_payload(),
                    timeout=3,
                )  # best effort, not scored

    def run(self) -> None:
        p: threading.Thread = threading.Thread(target=self.producer, daemon=True)
        p.start()
        ts: list[threading.Thread] = [
            threading.Thread(target=self.sender, args=(i,), daemon=True)
            for i in range(self.senders)
        ]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        p.join()
        self.db.close()


def main() -> None:
    e: Callable[..., Any] = os.environ.get
    scenario = e("SCENARIO", "otel")
    n = 3
    total_rate: float = float(e("RATE", "200"))
    duration: float = float(e("DURATION", "120"))
    retry_on = e("CLIENT_RETRY", "on").lower() == "on"
    ledger_dir = e("LEDGER_DIR", "/ledger")
    batch: int = int(e("BATCH", "500"))
    seed: int = int(e("SEED", "42"))
    retry_seconds: float = float(e("CLIENT_RETRY_SECONDS", "900"))
    drain_seconds: float = float(e("DRAIN_SECONDS", "600"))
    senders: int = int(e("SENDERS", "8"))
    only: str = e("SOURCE_INDEX", "")          # run a single source in this process (one process per source = one CPU core each)
    indexes = [int(only)] if only else list(range(1, n + 1))
    os.makedirs(name=ledger_dir, exist_ok=True)
    marker: str = os.path.join(ledger_dir, f"DONE-{only}" if only else "DONE")
    try:
        os.remove(path=marker)
    except FileNotFoundError:
        pass
    print(
        f"generator: scenario={scenario} sources={indexes} rate={total_rate}/s (all sources) duration={duration}s client_retry={retry_on} senders_per_source={senders}",
        flush=True,
    )
    sources: list[Source] = [
        Source(
            name=f"source-{i}",
            url=f"http://agent-{scenario}-{i}:4318",
            rate=total_rate / n,
            batch=batch,
            seed=seed,
            duration=duration,
            retry_on=retry_on,
            retry_seconds=retry_seconds,
            drain_seconds=drain_seconds,
            ledger_dir=ledger_dir,
            senders=senders,
        )
        for i in indexes
    ]
    for s in sources:
        s.start()
    t0: float = time.time()
    while any(s.is_alive() for s in sources):
        time.sleep(5)
        print(
            f"[{int(time.time() - t0):4d}s] "
            + " | ".join(
                f"{s.src}: gen={s.stats['generated']} ack={s.stats['acked']} q={s.q.qsize() * s.batch} err={s.stats['http_errors']}"
                for s in sources
            ),
            flush=True,
        )
    summary: dict[Any, dict[str, int]] = {s.src: s.stats for s in sources}
    with open(file=marker, mode="w") as f:
        json.dump(
            obj={"finished_ms": now_ms(), "scenario": scenario, "sources": summary},
            fp=f,
        )
    print(
        "generator done:",
        json.dumps(obj=summary),
        flush=True,
    )


if __name__ == "__main__":
    sys.exit(main())
