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

Dois modos (SYNC_MODE):
  * etcd     -> EtcdMutexService: a receita acima implementada no cliente, com log
                de cada passo (KEY_CREATED, WAIT, WAKE, KEY_DELETED). É o modo que
                deixa visível a ordem de espera e o papel das revisões.
  * etcd-api -> EtcdLockApiService: usa a API de concorrência do próprio etcd
                (serviço Lock: Lock/Unlock). No servidor, Lock executa
                concurrency.NewSession(lease) + concurrency.NewMutex(nome).Lock(),
                isto é, a MESMA receita, só que dentro do etcd.

Interface exposta ao resto da aplicação (separada da seção crítica):
    acquire(resource, request_id) / release(resource, request_id)
"""
import base64
import json
import threading
import time
from dataclasses import dataclass, field

import etcd3
from etcd3.exceptions import WatchTimedOut

from .http_util import post_json

ROOT = "/clustersync/locks"
API_ROOT = "/clustersync/api-locks"       # nome do mutex no modo etcd-api
SESSIONS_ROOT = "/clustersync/sessions"   # lease -> nó (só para exibir a fila)
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
    rounds: int = 0                # rodadas de espera (cada uma = watch + releitura da fila)
    timeouts: int = 0              # rodadas encerradas pela rede de segurança (sem evento)
    etcd_ops: int = 0              # operações do PROTOCOLO enviadas ao etcd (txn, get, watch...)
    inst_ops: int = 0              # leituras ao etcd feitas só para o log (instrumentação)


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
        # evidência de que a sessão ficou válida: nº de keepalives e o maior intervalo
        # entre dois keepalives bem-sucedidos (precisa ficar abaixo do TTL)
        self.keepalives = 0
        self.keepalive_errors = 0
        self._last_ok = time.monotonic()
        self.max_gap = 0.0
        # registro "lease -> nó", preso ao próprio lease: só para exibir a fila (show_queue.py)
        client.put(f"{SESSIONS_ROOT}/{self.lease_id:x}", name, lease=self.lease_id)
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
                now = time.monotonic()
                self.max_gap = max(self.max_gap, now - self._last_ok)
                self._last_ok = now
                self.keepalives += 1
            except Exception as exc:  # noqa: BLE001  (falha transitória: tenta de novo)
                self.keepalive_errors += 1
                self.log.log("KEEPALIVE_ERROR", lease=hex(self.lease_id), error=str(exc)[:120])

    def close(self):
        self._stop.set()
        # intervalo desde o último keepalive até o encerramento também conta
        self.max_gap = max(self.max_gap, time.monotonic() - self._last_ok)
        try:
            self.lease.revoke()  # apaga as chaves da sessão imediatamente
        except Exception:  # noqa: BLE001
            pass
        self.log.log("SESSION_CLOSED", lease=hex(self.lease_id), ttl=self.ttl, alive=self.alive,
                     keepalives=self.keepalives, keepalive_errors=self.keepalive_errors,
                     max_gap_s=round(self.max_gap, 3))


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

    def _wait_delete(self, key: str, start_rev: int) -> tuple[str, int | None]:
        """Espera a remoção de `key`. Observa a partir de start_rev (o create_revision
        do nosso pedido): se o predecessor foi apagado entre a leitura da fila e o
        início do watch, o evento é reproduzido e não ficamos presos.
        Devolve ("event", revisão da remoção) ou ("timeout", None)."""
        try:
            ev = self.client.watch_once(key, timeout=WATCH_POLL_S, start_revision=start_rev)
            return "event", getattr(ev, "mod_revision", None)
        except WatchTimedOut:
            return "timeout", None

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

            info.rounds += 1
            owner_key, owner_rev = ahead[0]
            pred_key, pred_rev = ahead[-1]
            info.predecessors.append(pred_key[len(pfx):])
            trace(
                "WAIT", resource=resource,
                owner=owner_key[len(pfx):], owner_rev=owner_rev,
                watching=pred_key[len(pfx):], watching_rev=pred_rev,
                ahead=len(ahead),
            )
            reason, delete_rev = self._wait_delete(pred_key, start_rev=my_rev)
            ops += 1
            if reason == "event":
                info.wakeups += 1
            else:
                info.timeouts += 1
            trace("WAKE", resource=resource, reason=reason, watched=pred_key[len(pfx):],
                  delete_rev=delete_rev)

    def release(self, resource: str, request_id: str, trace=None) -> int:
        trace = trace or (lambda *a, **k: None)
        key = f"{ROOT}/{resource}/{request_id}"
        resp = self.client.delete(key, return_response=True)
        # revision = revisão do etcd em que a chave foi apagada; o sucessor acorda com
        # um evento DELETE dessa mesma revisão (campo delete_rev do WAKE dele)
        trace("KEY_DELETED", resource=resource, key=key, deleted=resp.deleted >= 1,
              revision=resp.header.revision)
        return 1  # operações ao etcd

    def close(self):
        self.session.close()


def _b64(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


class EtcdLockApiService:
    """Mutex pela API de concorrência do etcd: serviço Lock (v3lockpb.Lock).

    Chamado pelo gateway HTTP/JSON do próprio etcd (mesma porta 2379):
        POST /v3/lock/lock    {"name": base64(nome), "lease": id}  -> {"key": base64(chave), "header": {...}}
        POST /v3/lock/unlock  {"key": base64(chave)}
    No servidor, Lock(nome, lease) faz
        s := concurrency.NewSession(cli, concurrency.WithLease(lease))
        m := concurrency.NewMutex(s, nome); m.Lock(ctx)
    ou seja, a mesma receita de EtcdMutexService executada dentro do etcd: chave
    <nome>/<lease em hex> presa ao lease, fila por create_revision e espera pela
    remoção da chave imediatamente anterior. A chamada Lock só retorna quando o
    pedido é o dono do lock.

    A chave do Mutex é derivada do LEASE: dois pedidos simultâneos do mesmo nó não
    podem usar a mesma sessão (teriam a mesma chave e entrariam juntos). Por isso
    cada pedido em andamento usa uma sessão exclusiva de um pool do nó; com um
    cliente por nó o pool fica com uma única sessão.
    """

    mode = "etcd-api"

    def __init__(self, host: str, port: int, ttl: int, name: str, log):
        self.name, self.log, self.ttl = name, log, ttl
        self.client = connect(host, port)
        self.base_url = f"http://{host}:{port}"
        self.session = Session(self.client, ttl, name, log)  # também usada pelo semáforo
        self._pool_lock = threading.Lock()  # protege só as estruturas internas do nó
        self._idle = [self.session]
        self._sessions = [self.session]
        self._held: dict[str, tuple[str, Session]] = {}

    def _take_session(self) -> Session:
        with self._pool_lock:
            if self._idle:
                return self._idle.pop()
        sess = Session(self.client, self.ttl, self.name, self.log)
        with self._pool_lock:
            self._sessions.append(sess)
        return sess

    def _give_session(self, sess: Session) -> None:
        with self._pool_lock:
            self._idle.append(sess)

    def _call(self, method: str, body: dict, timeout: float) -> dict:
        # retries=1: Lock não é repetido automaticamente (cada chamada é um pedido na fila)
        return post_json(f"{self.base_url}/v3/lock/{method}", body, timeout=timeout, retries=1)

    def acquire(self, resource: str, request_id: str, trace=None) -> AcquireInfo:
        trace = trace or (lambda *a, **k: None)
        sess = self._take_session()
        if not sess.alive:
            raise LockLost("sessão expirada")
        name = f"{API_ROOT}/{resource}"
        trace("LOCK_CALL", resource=resource, name=name, lease=hex(sess.lease_id))
        try:
            resp = self._call("lock", {"name": _b64(name), "lease": str(sess.lease_id)}, timeout=3600)
        except Exception:
            self._give_session(sess)
            raise
        key = base64.b64decode(resp["key"]).decode()
        with self._pool_lock:
            self._held[request_id] = (key, sess)
        # leitura só para o log/verificador (instrumentação, não faz parte do protocolo):
        # create_revision da chave = posição do pedido na fila do mutex
        _, meta = self.client.get(key)
        if meta is None:
            raise LockLost(f"chave {key} não existe após o Lock")
        info = AcquireInfo(key=key, create_rev=meta.create_revision, etcd_ops=1, inst_ops=1)
        trace("LOCK_GRANTED", resource=resource, key=key, create_rev=meta.create_revision,
              lease=hex(sess.lease_id), granted_rev=int(resp.get("header", {}).get("revision", 0)))
        return info

    def release(self, resource: str, request_id: str, trace=None) -> int:
        trace = trace or (lambda *a, **k: None)
        with self._pool_lock:
            key, sess = self._held.pop(request_id)
        try:
            resp = self._call("unlock", {"key": _b64(key)}, timeout=30)
        except Exception:
            # não devolve ao pool uma sessão cuja chave pode ter ficado no etcd:
            # revogar o lease apaga a chave e libera o próximo da fila
            with self._pool_lock:
                if sess in self._sessions:
                    self._sessions.remove(sess)
            sess.close()
            raise
        self._give_session(sess)
        trace("KEY_DELETED", resource=resource, key=key, via="Unlock",
              revision=int(resp.get("header", {}).get("revision", 0)))
        return 1  # operações do protocolo ao etcd (Unlock)

    def close(self):
        with self._pool_lock:
            sessions = list(self._sessions)
        for sess in sessions:
            sess.close()


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
    if mode == "etcd-api":
        return EtcdLockApiService(host, port, ttl, name, log)
    raise ValueError(f"SYNC_MODE inválido: {mode!r} (use 'etcd', 'etcd-api' ou 'none')")
