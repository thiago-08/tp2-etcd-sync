"""Semáforo distribuído com K permissões sobre o etcd (extensão do TP2).

É a generalização da receita do mutex (mutex.py), que corresponde a K = 1:

  * cada pedido cria a chave  /clustersync/semaphores/<etapa>/<request_id>,
    presa ao lease do nó; o create_revision define a posição na fila (FIFO);
  * o pedido TEM uma permissão quando existem MENOS DE K chaves com
    create_revision menor que a dele, isto é, quando está entre os K mais antigos;
  * senão espera até que alguma das K chaves da frente (as que hoje detêm as
    permissões) seja removida, e então reavalia a fila antes de entrar;
  * release = apagar a própria chave.

Por que não observar um único predecessor, como no mutex?
  Num mutex só o dono apaga a chave, então o predecessor imediato é sempre o
  próximo a sair. Num semáforo as K permissões são liberadas em QUALQUER ordem:
  se observássemos um predecessor específico, uma permissão poderia ficar ociosa
  enquanto ele ainda estivesse em uso. Por isso observamos as K chaves que
  detêm as permissões. Custo: uma liberação pode acordar vários esperando
  (todos reavaliam a fila); no mutex acorda só um. É o preço de K permissões
  liberadas fora de ordem.

Segurança (nunca mais de K simultâneos): a chave de um pedido existe do momento
em que ele entra na fila até o seu release. Se K+1 pedidos estivessem dentro ao
mesmo tempo, o de maior revisão teria visto as outras K chaves à sua frente na
leitura da fila (todas já criadas e ainda não liberadas) e não teria entrado.
"""
import json
import threading

import etcd3

from .mutex import WATCH_POLL_S, AcquireInfo, LockLost

ROOT = "/clustersync/semaphores"


class EtcdSemaphoreService:
    """acquire/release de uma permissão. Reaproveita cliente e sessão (lease)
    do EtcdMutexService do mesmo nó."""

    mode = "etcd"

    def __init__(self, mutex_service, permits: int, name: str, log):
        if permits < 1:
            raise ValueError("permits deve ser >= 1")
        self.client = mutex_service.client
        self.session = mutex_service.session
        self.permits = permits
        self.name = name
        self.log = log

    # ---------------------------------------------------------------- helpers
    def _queue(self, pfx: str) -> list[tuple[str, int]]:
        items = self.client.get_prefix(
            pfx, sort_order="ascend", sort_target="create", keys_only=True
        )
        return [(m.key.decode(), m.create_revision) for _, m in items]

    def _wait_any_delete(self, keys: list[str], start_rev: int) -> str:
        """Espera a remoção de QUALQUER uma das chaves. Um watch por chave, todos
        a partir de start_rev (create_revision do nosso pedido): se uma delas foi
        apagada entre a leitura da fila e o registro do watch, o evento é
        reproduzido e não ficamos presos."""
        woke = threading.Event()

        def on_event(event):
            if not isinstance(event, etcd3.events.PutEvent):  # DeleteEvent ou erro
                woke.set()

        watch_ids = []
        try:
            for k in keys:
                watch_ids.append(
                    self.client.add_watch_callback(k, on_event, start_revision=start_rev)
                )
            return "event" if woke.wait(WATCH_POLL_S) else "timeout"
        finally:
            for wid in watch_ids:
                try:
                    self.client.cancel_watch(wid)
                except Exception:  # noqa: BLE001
                    pass

    # -------------------------------------------------------------- interface
    def acquire(self, stage: str, request_id: str, trace=None) -> AcquireInfo:
        trace = trace or (lambda *a, **k: None)
        c = self.client
        pfx = f"{ROOT}/{stage}/"
        key = pfx + request_id
        k_permits = self.permits
        ops = 0

        if not self.session.alive:
            raise LockLost("sessão expirada")

        # 1) entra na fila (transação idempotente, igual ao mutex)
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
        trace("KEY_CREATED", resource=stage, key=key, create_rev=my_rev,
              lease=hex(self.session.lease_id))

        # 2) espera até haver menos de K pedidos à frente
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
            if len(ahead) < k_permits:
                info.etcd_ops = ops
                return info  # há permissão livre: estou entre os K mais antigos

            holders = ahead[:k_permits]  # as K chaves que detêm as permissões agora
            trace(
                "WAIT", resource=stage, permits=k_permits, ahead=len(ahead),
                holders=[h[len(pfx):] for h, _ in holders],
                holder_revs=[r for _, r in holders],
            )
            reason = self._wait_any_delete([h for h, _ in holders], start_rev=my_rev)
            ops += len(holders)
            info.predecessors.extend(h[len(pfx):] for h, _ in holders)
            if reason == "event":
                info.wakeups += 1
            trace("WAKE", resource=stage, reason=reason)

    def release(self, stage: str, request_id: str, trace=None) -> int:
        trace = trace or (lambda *a, **k: None)
        key = f"{ROOT}/{stage}/{request_id}"
        deleted = self.client.delete(key)
        trace("KEY_DELETED", resource=stage, key=key, deleted=bool(deleted))
        return 1


class NoSemaphoreService:
    """Proteção DESLIGADA (SYNC_MODE=none): não limita ninguém."""

    mode = "none"

    def __init__(self, permits: int):
        self.permits = permits

    def acquire(self, stage: str, request_id: str, trace=None) -> AcquireInfo:
        return AcquireInfo(key=f"{stage}/{request_id}")

    def release(self, stage: str, request_id: str, trace=None) -> int:
        return 0


def make_semaphore_service(mode: str, mutex_service, permits: int, name: str, log):
    if mode == "none":
        return NoSemaphoreService(permits)
    return EtcdSemaphoreService(mutex_service, permits, name, log)
