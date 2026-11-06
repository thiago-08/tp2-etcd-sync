#!/usr/bin/env python3
"""Mostra a fila do lock de R no etcd (para a demonstração ao vivo).

    python scripts/show_queue.py            # uma vez
    python scripts/show_queue.py --watch    # atualiza a cada 0,3 s

Posição 0 = dono do lock (menor create_revision). Os demais esperam, cada um
observando o predecessor imediato.
"""
import argparse
import json
import os
import time

import etcd3


def snapshot(client, resource):
    pfx = f"/clustersync/locks/{resource}/"
    rows = []
    items = client.get_prefix(pfx, sort_order="ascend", sort_target="create")
    for pos, (val, meta) in enumerate(items):
        info = json.loads(val) if val else {}
        rows.append((pos, meta.key.decode()[len(pfx):], meta.create_revision,
                     hex(meta.lease_id), info.get("node", "?")))
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--resource", default="R")
    ap.add_argument("--host", default=os.environ.get("ETCD_HOST", "localhost"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("ETCD_PORT", 2379)))
    ap.add_argument("--watch", action="store_true")
    a = ap.parse_args()
    client = etcd3.client(host=a.host, port=a.port)
    while True:
        rows = snapshot(client, a.resource)
        if a.watch:
            print("\033[2J\033[H", end="")
        print(f"Fila do recurso {a.resource} ({len(rows)} pedido(s))")
        print(f"{'pos':<4} {'request_id':<10} {'create_rev':<11} {'nó':<8} lease")
        for pos, rid, rev, lease, node in rows:
            print(f"{pos:<4} {rid:<10} {rev:<11} {node:<8} {lease}" + ("   <- DONO" if pos == 0 else ""))
        if not a.watch:
            return
        time.sleep(0.3)


if __name__ == "__main__":
    main()
