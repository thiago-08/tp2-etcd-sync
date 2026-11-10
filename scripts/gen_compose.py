#!/usr/bin/env python3
"""Gera um docker-compose para N nós do Cluster Sync e M clientes.

Uso:  python scripts/gen_compose.py --nodes 5 --clients 5 > docker-compose.yml
O cliente i conhece apenas o nó ((i-1) mod N)+1.
A imagem da aplicação é construída UMA vez (só o serviço observer tem `build`); os
demais serviços reutilizam a mesma imagem (`pull_policy: never`). Com `build` em todos
os serviços, o Docker exportava a mesma imagem uma vez por serviço (~10 s cada).
Os parâmetros do experimento vêm de variáveis de ambiente do host
(REQUESTS, THINK_MIN, SYNC_MODE, ... ) — veja scripts/run_experiment.py.
"""
import argparse

ENV_PASSTHROUGH = [
    "SYNC_MODE", "RUN_MODE", "SEED", "RUN_ID", "CS_MIN", "CS_MAX",
    "SEMAPHORE_K", "STAGE_MIN", "STAGE_MAX",
]
CLIENT_PASSTHROUGH = [
    "SEED", "RUN_ID", "START_AT", "REQUESTS", "THINK_MIN", "THINK_MAX",
    "HEAVY_CLIENT", "HEAVY_REQUESTS", "HEAVY_THINK_MIN", "HEAVY_THINK_MAX",
]
DEFAULTS = {
    "SYNC_MODE": "etcd", "RUN_MODE": "docker", "SEED": "1", "RUN_ID": "docker", "CS_MIN": "0.2", "CS_MAX": "1.0",
    "START_AT": "0", "REQUESTS": "10", "THINK_MIN": "1.0", "THINK_MAX": "5.0",
    "SEMAPHORE_K": "0", "STAGE_MIN": "0.2", "STAGE_MAX": "1.0",
    "HEAVY_CLIENT": "", "HEAVY_REQUESTS": "30", "HEAVY_THINK_MIN": "0.0", "HEAVY_THINK_MAX": "0.3",
}


def var(name: str) -> str:
    return f"${{{name}:-{DEFAULTS[name]}}}"


IMAGE = "tp2-etcd-sync:latest"


def common(command: str, build: bool = False) -> list[str]:
    head = ["    build: .", f"    image: {IMAGE}"] if build else [
        f"    image: {IMAGE}",
        "    pull_policy: never  # imagem local, construída pelo serviço observer",
    ]
    return [
        *head,
        '    user: "${LOCAL_UID:-1000}:${LOCAL_GID:-1000}"',
        f"    command: python -m clustersync.{command}",
        "    volumes:",
        "      - ./logs:/app/logs",
    ]


def generate(nodes: int, clients: int) -> str:
    L = [
        "# GERADO por scripts/gen_compose.py — não edite à mão.",
        "name: tp2-etcd-sync",
        "services:",
        "  etcd:",
        "    image: quay.io/coreos/etcd:v3.5.17",
        "    command: [etcd, --name, s1, --data-dir, /etcd-data,",
        "              --listen-client-urls, 'http://0.0.0.0:2379',",
        "              --advertise-client-urls, 'http://etcd:2379']",
        "    ports:",
        '      - "${ETCD_HOST_PORT:-2379}:2379"',
        "",
        "  observer:",
        *common("observer", build=True),
        "    environment:",
        "      OBSERVER_PORT: 8000",
        "      LOG_DIR: /app/logs/${RUN_ID:-docker}",
        "",
    ]
    for i in range(1, nodes + 1):
        L += [f"  node{i}:", *common("node"),
              "    depends_on: [etcd, observer]",
              "    environment:",
              f"      NODE_ID: node{i}", "      NODE_PORT: 9000",
              "      ETCD_HOST: etcd", "      ETCD_PORT: 2379",
              "      OBSERVER_URL: http://observer:8000",
              "      LOG_DIR: /app/logs/${RUN_ID:-docker}"]
        L += [f"      {k}: {var(k)}" for k in ENV_PASSTHROUGH]
        L.append("")
    for j in range(1, clients + 1):
        target = (j - 1) % nodes + 1
        L += [f"  client{j}:", *common("client"),
              f"    depends_on: [node{target}]",
              "    environment:",
              f"      CLIENT_ID: c{j}", f"      NODE_URL: http://node{target}:9000",
              "      LOG_DIR: /app/logs/${RUN_ID:-docker}"]
        L += [f"      {k}: {var(k)}" for k in CLIENT_PASSTHROUGH]
        L.append("")
    return "\n".join(L)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=5)
    ap.add_argument("--clients", type=int, default=5)
    a = ap.parse_args()
    print(generate(a.nodes, a.clients))
