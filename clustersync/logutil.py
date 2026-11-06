"""Log estruturado: uma linha JSON por evento + linha legível no stdout.

Cada componente (observer, node1, c1, ...) grava em LOG_DIR/<componente>.jsonl.
Campos fixos: ts (relógio físico, só informativo), mono (relógio monotônico
do PRÓPRIO processo), component, event.
"""
import json
import os
import threading
import time

_SKIP = {"ts", "mono", "component", "event"}


class JsonlLogger:
    def __init__(self, component: str, log_dir: str | None = None):
        self.component = component
        self._lock = threading.Lock()
        self._fh = None
        log_dir = log_dir or os.environ.get("LOG_DIR")
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
            path = os.path.join(log_dir, f"{component}.jsonl")
            self._fh = open(path, "a", buffering=1, encoding="utf-8")

    def log(self, event: str, **fields) -> dict:
        rec = {
            "ts": round(time.time(), 6),
            "mono": round(time.monotonic(), 6),
            "component": self.component,
            "event": event,
        }
        rec.update(fields)
        line = json.dumps(rec, ensure_ascii=False)
        pretty = " ".join(f"{k}={v}" for k, v in rec.items() if k not in _SKIP)
        stamp = time.strftime("%H:%M:%S", time.localtime(rec["ts"]))
        ms = int((rec["ts"] % 1) * 1000)
        with self._lock:
            if self._fh:
                self._fh.write(line + "\n")
            print(f"{stamp}.{ms:03d} {self.component:<9} {event:<12} {pretty}", flush=True)
        return rec

    def close(self):
        with self._lock:
            if self._fh:
                self._fh.close()
                self._fh = None
