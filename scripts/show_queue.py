#!/usr/bin/env python3
"""Mostra a fila do lock de R no etcd (para a demonstração ao vivo).

    python scripts/show_queue.py                 # uma vez (modo etcd: receita no cliente)
    python scripts/show_queue.py --watch         # atualiza a cada 0,3 s
    python scripts/show_queue.py --api --watch   # fila do modo etcd-api (serviço Lock do etcd)

Posição 0 = dono do lock (menor create_revision). Os demais esperam, cada um
observando a chave imediatamente anterior (o predecessor).
  * modo etcd     : chave /clustersync/locks/R/<request_id>, valor = {nó, request_id}
  * modo etcd-api : chave /clustersync/api-locks/R/<lease em hex>, criada pelo próprio
                    etcd (concurrency.Mutex); o nó é descoberto pelo registro
                    /clustersync/sessions/<lease> que cada sessão grava ao iniciar.
"""
import argparse
import json
import os
import time

import etcd3


def sessions(client) -> dict[str, str]:
    pfx = "/clustersync/sessions/"
    return {m.key.decode()[len(pfx):]: v.decode() for v, m in client.get_prefix(pfx)}


def snapshot(client, resource, api: bool):
    pfx = f"/clustersync/{'api-locks' if api else 'locks'}/{resource}/"
    owners = sessions(client)
    rows = []
    items = client.get_prefix(pfx, sort_order="ascend", sort_target="create")
    for pos, (val, meta) in enumerate(items):
        lease = f"{meta.lease_id:x}"
        if api:
            name, node = meta.key.decode()[len(pfx):], owners.get(lease, "?")
        else:
            info = json.loads(val) if val else {}
            name, node = meta.key.decode()[len(pfx):], info.get("node", owners.get(lease, "?"))
        rows.append((pos, name, meta.create_revision, meta.mod_revision, node, hex(meta.lease_id)))
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--resource", default="R")
    ap.add_argument("--api", action="store_true", help="fila do modo --sync etcd-api")
    ap.add_argument("--host", default=os.environ.get("ETCD_HOST", "localhost"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("ETCD_PORT", 2379)))
    ap.add_argument("--watch", action="store_true")
    a = ap.parse_args()
    client = etcd3.client(host=a.host, port=a.port)
    label = "chave (lease)" if a.api else "request_id"
    while True:
        rows = snapshot(client, a.resource, a.api)
        if a.watch:
            print("\033[2J\033[H", end="")
        print(f"Fila do recurso {a.resource} ({len(rows)} pedido(s)) — modo {'etcd-api' if a.api else 'etcd'}")
        print(f"{'pos':<4} {label:<18} {'create_rev':<11} {'mod_rev':<8} {'nó':<8} lease")
        for pos, name, rev, mod, node, lease in rows:
            print(f"{pos:<4} {name:<18} {rev:<11} {mod:<8} {node:<8} {lease}" + ("   <- DONO" if pos == 0 else ""))
        if not a.watch:
            return
        time.sleep(0.3)


if __name__ == "__main__":
    main()
