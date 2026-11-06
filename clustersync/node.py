"""Elemento do Cluster Sync.

Recebe pedidos de UM ou mais clientes (HTTP), obtém a autorização via
acquire(R, request_id), executa a seção crítica simulada (sleep aleatório,
com BEGIN/END registrados no observador), libera via release(R, request_id)
e responde COMMITTED. A sincronização (mutex.py) fica separada da seção
crítica (CriticalSection abaixo) — no TP3 ela vira acesso ao Cluster Store.
"""
import json
import random
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .config import env_float, env_int, env_str
from .http_util import post_json, wait_http
from .logutil import JsonlLogger
from .mutex import make_lock_service


class CriticalSection:
    """Seção crítica simulada: sleep aleatório. Não conhece o mecanismo de lock."""

    def __init__(self, observer_url: str, cs_min: float, cs_max: float, rng, node_id: str, log):
        self.observer_url = observer_url
        self.cs_min, self.cs_max = cs_min, cs_max
        self.rng = rng
        self._rng_lock = threading.Lock()
        self.node_id = node_id
        self.log = log

    def run(self, client_id: str, request_id: str) -> float:
        meta = {"node": self.node_id, "client": client_id, "request_id": request_id}
        # BEGIN é registrado ANTES do sleep (a aplicação espera a confirmação)
        post_json(f"{self.observer_url}/begin", meta)
        self.log.log("ENTER", **meta)
        with self._rng_lock:
            duration = self.rng.uniform(self.cs_min, self.cs_max)
        time.sleep(duration)
        # END é registrado DEPOIS do sleep e antes do release
        post_json(f"{self.observer_url}/end", meta)
        self.log.log("EXIT", cs_s=round(duration, 6), **meta)
        return duration


class ClusterSyncNode:
    def __init__(self, node_id: str, lock, cs: CriticalSection, resource: str, log):
        self.node_id, self.lock, self.cs, self.resource, self.log = node_id, lock, cs, resource, log

    def handle_request(self, client_id: str, request_id: str) -> dict:
        meta = {"node": self.node_id, "client": client_id, "request_id": request_id}
        t0 = time.monotonic()
        self.log.log("REQUEST", **meta)

        def trace(event, **fields):
            self.log.log(event, node=self.node_id, request_id=request_id, **fields)

        info = self.lock.acquire(self.resource, request_id, trace=trace)
        wait_s = time.monotonic() - t0  # espera medida no próprio processo (monotônico)
        self.log.log("ACQUIRE", wait_s=round(wait_s, 6), create_rev=info.create_rev,
                     queued_ahead=info.queued_ahead, wakeups=info.wakeups,
                     etcd_ops=info.etcd_ops, **meta)
        try:
            self.cs.run(client_id, request_id)
        finally:
            rel_ops = self.lock.release(self.resource, request_id, trace=trace)
            self.log.log("RELEASE", etcd_ops=rel_ops, **meta)
        self.log.log("COMMITTED", **meta)
        return {"status": "COMMITTED", "request_id": request_id, "node": self.node_id,
                "wait_s": round(wait_s, 6)}


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128


def make_handler(node: ClusterSyncNode):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
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
                self._reply(200, {"ok": True, "node": node.node_id})
            else:
                self._reply(404, {"error": "not found"})

        def do_POST(self):
            if self.path != "/request":
                return self._reply(404, {"error": "not found"})
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length) or b"{}")
            try:
                # cada pedido roda na sua thread; pedidos simultâneos do mesmo nó
                # viram chaves distintas na fila do etcd (são enfileirados pelo protocolo)
                result = node.handle_request(body["client_id"], body["request_id"])
                self._reply(200, result)
            except Exception as exc:  # noqa: BLE001
                node.log.log("ERROR", request_id=body.get("request_id"), error=repr(exc)[:200])
                self._reply(500, {"status": "ERROR", "error": str(exc)})

    return Handler


def main():
    node_id = env_str("NODE_ID", "node1")
    port = env_int("NODE_PORT", 9000)
    mode = env_str("SYNC_MODE", "etcd")
    seed = env_str("SEED", "1")
    resource = env_str("RESOURCE", "R")
    cs_min, cs_max = env_float("CS_MIN", 0.2), env_float("CS_MAX", 1.0)
    observer_url = env_str("OBSERVER_URL", "http://localhost:8000")

    log = JsonlLogger(node_id)
    wait_http(f"{observer_url}/health")
    lock = make_lock_service(mode, env_str("ETCD_HOST", "localhost"), env_int("ETCD_PORT", 2379),
                             env_int("SESSION_TTL", 10), node_id, log)
    rng = random.Random(f"{seed}:{node_id}")
    cs = CriticalSection(observer_url, cs_min, cs_max, rng, node_id, log)
    node = ClusterSyncNode(node_id, lock, cs, resource, log)

    server = Server(("0.0.0.0", port), make_handler(node))
    log.log("NODE_START", port=port, sync_mode=mode, seed=seed, resource=resource,
            cs_min=cs_min, cs_max=cs_max)

    def stop(*_):
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        server.serve_forever()
    finally:
        lock.close()
        log.log("NODE_STOP")
        log.close()


if __name__ == "__main__":
    main()
