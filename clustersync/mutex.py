"""Mutex distribuído sobre o etcd (receita do concurrency.Mutex do cliente Go).

Ideia central
-------------
* Cada NÓ do Cluster Sync abre uma *sessão*: um lease (TTL) mantido vivo por
  keepalive. Se o nó morrer, o lease expira e o etcd apaga as chaves dele.
* Cada PEDIDO cria uma chave  /clustersync/locks/<recurso>/<request_id>  presa
  ao lease da sessão. O etcd atribui a ela um create_revision (número global,
  estritamente crescente) que define a ordem da fila.
* Quem tem o MENOR create_revision é o dono do lock. Os demais observam
  (watch) apenas a chave do seu PREDECESSOR imediato — assim uma liberação
  acorda um único concorrente (sem "efeito manada").
* Ao ser notificado, o concorrente reavalia a fila antes de entrar.

Interface exposta ao resto da aplicação (separada da seção crítica):
    acquire(resource, request_id) / release(resource, request_id)
"""
import json
import threading
from dataclasses import dataclass, field

import etcd3
from etcd3.exceptions import WatchTimedOut

ROOT = "/clustersync/locks"
WATCH_POLL_S = 5.0  # rede de segurança: reavalia a fila mesmo sem evento


class LockLost(RuntimeError):
    """A sessão (lease) expirou ou a chave do pedido sumiu antes da aquisição."""


@dataclass
class AcquireInfo:
    key: str = ""
    create_rev: int | None = None
    queued_ahead: int = 0          # quantos pedidos estavam à frente ao entrar na fila
    predecessors: list = field(default_factory=list)
    wakeups: int = 0               # quantas notificações de watch recebidas
    etcd_ops: int = 0              # operações enviadas ao etcd (txn, get, watch...)


def connect(host: str, port: int, retries: int = 60):
    """Cria o cliente e espera o etcd responder (compose sobe tudo em paralelo)."""
    import time

    client = etcd3.client(host=host, port=port)
    last = None
    for _ in range(retries):
        try:
            client.status()
            return client
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(1)
    raise RuntimeError(f"etcd {host}:{port} indisponível: {last}")


class Session:
    """Lease com keepalive em thread própria (equivale a concurrency.Session)."""

    def __init__(self, client, ttl: int, name: str, log):
        self.client = client
        self.ttl = ttl
        self.name = name
        self.log = log
        self.lease = client.lease(ttl)
        self.alive = True
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._keepalive, daemon=True)
        self._thread.start()
        self.log.log("SESSION_START", lease=hex(self.lease_id), ttl=ttl)

    @property
    def lease_id(self) -> int:
        return self.lease.id

    def _keepalive(self):
        interval = max(self.ttl / 3.0, 0.5)
        while not self._stop.wait(interval):
            try:
                resp = self.lease.refresh()
                remaining = resp[0].TTL if resp else 0
                if remaining <= 0:
                    self.alive = False
                    self.log.log("SESSION_EXPIRED", lease=hex(self.lease_id))
                    return
            except Exception as exc:  # noqa: BLE001  (falha transitória: tenta de novo)
                self.log.log("KEEPALIVE_ERROR", error=str(exc)[:120])

    def close(self):
        self._stop.set()
        try:
            self.lease.revoke()  # apaga as chaves da sessão imediatamente
        except Exception:  # noqa: BLE001
            pass
        self.log.log("SESSION_CLOSED", lease=hex(self.lease_id))


class EtcdMutexService:
    """acquire/release sobre o etcd; uma instância por nó do Cluster Sync."""

    mode = "etcd"

    def __init__(self, host: str, port: int, ttl: int, name: str, log):
        self.name = name
        self.log = log
        self.client = connect(host, port)
        self.session = Session(self.client, ttl, name, log)

    # ---------------------------------------------------------------- helpers
    def _queue(self, pfx: str) -> list[tuple[str, int]]:
        """Fila do recurso: [(chave, create_revision)] em ordem crescente de criação."""
        items = self.client.get_prefix(
            pfx, sort_order="ascend", sort_target="create", keys_only=True
        )
        return [(m.key.decode(), m.create_revision) for _, m in items]

    def _wait_delete(self, key: str, start_rev: int) -> str:
        """Espera a remoção de `key`. Observa a partir de start_rev (o create_revision
        do nosso pedido): se o predecessor foi apagado entre a leitura da fila e o
        início do watch, o evento é reproduzido e não ficamos presos."""
        try:
            self.client.watch_once(key, timeout=WATCH_POLL_S, start_revision=start_rev)
            return "event"
        except WatchTimedOut:
            return "timeout"

    # -------------------------------------------------------------- interface
    def acquire(self, resource: str, request_id: str, trace=None) -> AcquireInfo:
        trace = trace or (lambda *a, **k: None)
        c = self.client
        pfx = f"{ROOT}/{resource}/"
        key = pfx + request_id
        ops = 0

        if not self.session.alive:
            raise LockLost("sessão expirada")

        # 1) entra na fila: cria a chave se ainda não existir (idempotente)
        value = json.dumps({"node": self.name, "request_id": request_id})
        c.transaction(
            compare=[c.transactions.create(key) == 0],
            success=[c.transactions.put(key, value, lease=self.session.lease_id)],
            failure=[c.transactions.get(key)],
        )
        ops += 1
        _, meta = c.get(key)
        ops += 1
        if meta is None:
            raise LockLost(f"chave {key} não existe após a criação")
        my_rev = meta.create_revision
        info = AcquireInfo(key=key, create_rev=my_rev)
        trace("KEY_CREATED", resource=resource, key=key, create_rev=my_rev, lease=hex(self.session.lease_id))

        # 2) espera até ser o menor create_revision da fila
        first = True
        while True:
            if not self.session.alive:
                raise LockLost("sessão expirada durante a espera")
            queue = self._queue(pfx)
            ops += 1
            if key not in [k for k, _ in queue]:
                raise LockLost(f"chave {key} removida durante a espera")
            ahead = [(k, r) for k, r in queue if r < my_rev]
            if first:
                info.queued_ahead = len(ahead)
                first = False
            if not ahead:
                info.etcd_ops = ops
                return info

            owner_key, owner_rev = ahead[0]
            pred_key, pred_rev = ahead[-1]
            info.predecessors.append(pred_key[len(pfx):])
            trace(
                "WAIT", resource=resource,
                owner=owner_key[len(pfx):], owner_rev=owner_rev,
                watching=pred_key[len(pfx):], watching_rev=pred_rev,
                ahead=len(ahead),
            )
            reason = self._wait_delete(pred_key, start_rev=my_rev)
            ops += 1
            if reason == "event":
                info.wakeups += 1
            trace("WAKE", resource=resource, reason=reason, watched=pred_key[len(pfx):])

    def release(self, resource: str, request_id: str, trace=None) -> int:
        trace = trace or (lambda *a, **k: None)
        key = f"{ROOT}/{resource}/{request_id}"
        deleted = self.client.delete(key)
        trace("KEY_DELETED", resource=resource, key=key, deleted=bool(deleted))
        return 1  # operações ao etcd

    def close(self):
        self.session.close()


class NoLockService:
    """Proteção DESLIGADA: acquire/release não coordenam nada.
    Serve só para provar que o observador detecta acessos simultâneos."""

    mode = "none"

    def __init__(self, name: str, log):
        self.name = name
        self.log = log

    def acquire(self, resource: str, request_id: str, trace=None) -> AcquireInfo:
        return AcquireInfo(key=f"{resource}/{request_id}")

    def release(self, resource: str, request_id: str, trace=None) -> int:
        return 0

    def close(self):
        pass


def make_lock_service(mode: str, host: str, port: int, ttl: int, name: str, log):
    if mode == "none":
        return NoLockService(name, log)
    if mode == "etcd":
        return EtcdMutexService(host, port, ttl, name, log)
    raise ValueError(f"SYNC_MODE inválido: {mode!r} (use 'etcd' ou 'none')")
