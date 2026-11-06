"""Observador: recebe BEGIN/END de todos os nós e conta acessos ativos a R.

Só REGISTRA e ACUSA sobreposição. Não concede permissão, não bloqueia ninguém
e não é mutex: o BEGIN de qualquer nó é sempre aceito imediatamente.
A ordem global dos eventos é a ordem de chegada aqui (campo `seq`), o que
dispensa comparar relógios físicos de máquinas diferentes.
"""
import json
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .config import env_int
from .logutil import JsonlLogger


class Observer:
    def __init__(self, log: JsonlLogger):
        self.log = log
        self._lock = threading.Lock()
        self.active: dict[str, str] = {}  # request_id -> node
        self.seq = 0
        self.max_active = 0
        self.violations = 0
        self.begins = 0
        self.ends = 0

    def begin(self, rec: dict) -> dict:
        rid = rec["request_id"]
        with self._lock:
            if rid in self.active:  # retransmissão do mesmo BEGIN: não conta duas vezes
                self.log.log("DUPLICATE_BEGIN", request_id=rid)
                return {"seq": self.seq, "active": len(self.active), "overlap": False}
            self.seq += 1
            others = sorted(self.active)
            self.active[rid] = rec.get("node", "?")
            self.begins += 1
            n = len(self.active)
            self.max_active = max(self.max_active, n)
            overlap = n > 1
            if overlap:
                self.violations += 1
            self.log.log("BEGIN", seq=self.seq, request_id=rid, node=rec.get("node"),
                         client=rec.get("client"), active=n, overlap=overlap)
            if overlap:
                self.log.log("OVERLAP", seq=self.seq, request_id=rid, concurrent_with=others)
            return {"seq": self.seq, "active": n, "overlap": overlap}

    def end(self, rec: dict) -> dict:
        rid = rec["request_id"]
        with self._lock:
            if rid not in self.active:
                self.log.log("END_WITHOUT_BEGIN", request_id=rid)
                return {"seq": self.seq, "active": len(self.active)}
            self.seq += 1
            del self.active[rid]
            self.ends += 1
            self.log.log("END", seq=self.seq, request_id=rid, node=rec.get("node"),
                         client=rec.get("client"), active=len(self.active))
            return {"seq": self.seq, "active": len(self.active)}

    def stats(self) -> dict:
        with self._lock:
            return {"active": len(self.active), "max_active": self.max_active,
                    "violations": self.violations, "begins": self.begins, "ends": self.ends}


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128


def make_handler(obs: Observer):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # silencia o log padrão do http.server
            pass

        def _reply(self, code: int, body: dict):
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == "/health":
                self._reply(200, {"ok": True})
            elif self.path == "/stats":
                self._reply(200, obs.stats())
            else:
                self._reply(404, {"error": "not found"})

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length) or b"{}")
            if self.path == "/begin":
                self._reply(200, obs.begin(body))
            elif self.path == "/end":
                self._reply(200, obs.end(body))
            else:
                self._reply(404, {"error": "not found"})

    return Handler


def main():
    port = env_int("OBSERVER_PORT", 8000)
    log = JsonlLogger("observer")
    obs = Observer(log)
    server = Server(("0.0.0.0", port), make_handler(obs))
    log.log("OBSERVER_START", port=port)

    def stop(*_):
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        server.serve_forever()
    finally:
        log.log("OBSERVER_STOP", **obs.stats())
        log.close()


if __name__ == "__main__":
    main()
