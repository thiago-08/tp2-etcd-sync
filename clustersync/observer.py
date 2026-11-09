"""Observador: recebe BEGIN/END de todos os nós e conta acessos ativos a R
(e à etapa do semáforo, quando houver).

Só REGISTRA e ACUSA sobreposição / excesso do limite. Não concede permissão, não bloqueia ninguém
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
    """Conta acessos ativos POR RECURSO. R tem limite 1 (exclusão mútua); a etapa
    do semáforo (S) tem limite K. O limite vem no próprio BEGIN; o verificador
    reconfere os limites de forma independente a partir da configuração dos nós."""

    def __init__(self, log: JsonlLogger):
        self.log = log
        self._lock = threading.Lock()
        self.seq = 0
        self.res: dict[str, dict] = {}

    def _r(self, resource: str, limit: int) -> dict:
        r = self.res.setdefault(resource, {"active": {}, "max_active": 0, "violations": 0,
                                           "begins": 0, "ends": 0, "limit": limit})
        r["limit"] = limit
        return r

    def begin(self, rec: dict) -> dict:
        rid, resource = rec["request_id"], rec.get("resource", "R")
        with self._lock:
            r = self._r(resource, int(rec.get("limit", 1)))
            if rid in r["active"]:  # retransmissão do mesmo BEGIN: não conta duas vezes
                self.log.log("DUPLICATE_BEGIN", request_id=rid, resource=resource)
                return {"seq": self.seq, "active": len(r["active"]), "overlap": False}
            self.seq += 1
            others = sorted(r["active"])
            r["active"][rid] = rec.get("node", "?")
            r["begins"] += 1
            n = len(r["active"])
            r["max_active"] = max(r["max_active"], n)
            overlap = n > r["limit"]  # acima do limite do recurso (1 em R, K na etapa S)
            if overlap:
                r["violations"] += 1
            self.log.log("BEGIN", seq=self.seq, resource=resource, limit=r["limit"], request_id=rid,
                         node=rec.get("node"), client=rec.get("client"), active=n, overlap=overlap)
            if overlap:
                self.log.log("OVERLAP", seq=self.seq, resource=resource, limit=r["limit"],
                             request_id=rid, active=n, concurrent_with=others)
            return {"seq": self.seq, "active": n, "overlap": overlap}

    def end(self, rec: dict) -> dict:
        rid, resource = rec["request_id"], rec.get("resource", "R")
        with self._lock:
            r = self._r(resource, int(rec.get("limit", 1)))
            if rid not in r["active"]:
                self.log.log("END_WITHOUT_BEGIN", request_id=rid, resource=resource)
                return {"seq": self.seq, "active": len(r["active"])}
            self.seq += 1
            del r["active"][rid]
            r["ends"] += 1
            self.log.log("END", seq=self.seq, resource=resource, limit=r["limit"], request_id=rid,
                         node=rec.get("node"), client=rec.get("client"), active=len(r["active"]))
            return {"seq": self.seq, "active": len(r["active"])}

    def stats(self) -> dict:
        with self._lock:
            per = {name: {"active": len(r["active"]), "max_active": r["max_active"],
                          "violations": r["violations"], "begins": r["begins"],
                          "ends": r["ends"], "limit": r["limit"]} for name, r in self.res.items()}
            base = per.get("R", {"active": 0, "max_active": 0, "violations": 0, "begins": 0, "ends": 0})
            return {**{k: base[k] for k in ("active", "max_active", "violations", "begins", "ends")},
                    "resources": per}


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
