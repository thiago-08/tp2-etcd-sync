#!/usr/bin/env python3
"""Orquestra uma execução completa: sobe observador + nós + clientes, espera
terminar, e roda o verificador.

Exemplos
  python scripts/run_experiment.py --scenario normal --requests 10
  python scripts/run_experiment.py --scenario contention --requests 20
  python scripts/run_experiment.py --scenario contention --sync none      # proteção desligada
  python scripts/run_experiment.py --scenario uneven --heavy-client c1
  python scripts/run_experiment.py --mode docker --nodes 10 --clients 5
  python scripts/run_experiment.py --scenario contention --semaphore 2   # extensão: semáforo K=2
  python scripts/run_experiment.py --scenario contention --semaphore 3 --stage-min 1 --stage-max 2

--mode local  : processos Python independentes na máquina (precisa do etcd em localhost:2379)
--mode docker : um contêiner por processo via docker compose (inclui o etcd)

Cenários
  normal     : 1 a 5 s entre pedidos (padrão do enunciado)
  contention : sem espera entre pedidos (alta contenção)
  uneven     : um cliente pesado (mais pedidos, quase sem espera) e os demais normais
"""
import argparse
import datetime
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import gen_compose  # noqa: E402
import verify  # noqa: E402
from clustersync.http_util import get_json, wait_http  # noqa: E402

SCENARIOS = {
    "normal": {"think": (1.0, 5.0), "heavy": False},
    "contention": {"think": (0.0, 0.0), "heavy": False},
    "uneven": {"think": (1.0, 5.0), "heavy": True},
}


def build_env(a, run_id: str, start_at: float) -> dict:
    think = SCENARIOS[a.scenario]["think"]
    tmin = a.think_min if a.think_min is not None else think[0]
    tmax = a.think_max if a.think_max is not None else think[1]
    heavy = SCENARIOS[a.scenario]["heavy"]
    return {
        "SYNC_MODE": a.sync, "SEED": str(a.seed), "RUN_ID": run_id,
        "CS_MIN": str(a.cs_min), "CS_MAX": str(a.cs_max),
        "START_AT": f"{start_at:.3f}", "REQUESTS": str(a.requests),
        "THINK_MIN": str(tmin), "THINK_MAX": str(tmax),
        "HEAVY_CLIENT": a.heavy_client if heavy else "",
        "HEAVY_REQUESTS": str(a.heavy_requests or min(50, a.requests * 3)),
        "HEAVY_THINK_MIN": "0.0", "HEAVY_THINK_MAX": "0.3",
        "SEMAPHORE_K": str(a.semaphore),
        "STAGE_MIN": str(a.stage_min if a.stage_min is not None else a.cs_min),
        "STAGE_MAX": str(a.stage_max if a.stage_max is not None else a.cs_max),
    }


def progress_loop(procs, obs_url, total_hint, quiet):
    """Espera todos os clientes terminarem, imprimindo o progresso do observador."""
    last = 0.0
    while any(p.poll() is None for p in procs):
        time.sleep(0.5)
        if not quiet and time.monotonic() - last > 5:
            last = time.monotonic()
            try:
                s = get_json(f"{obs_url}/stats")
                print(f"  ... acessos concluídos: {s['ends']}/{total_hint}  "
                      f"ativos: {s['active']}  sobreposições: {s['violations']}", flush=True)
            except Exception:  # noqa: BLE001
                pass


def run_local(a, run_id: str, log_dir: Path) -> None:
    import etcd3
    if a.sync == "etcd":
        try:
            etcd3.client(host=a.etcd_host, port=a.etcd_port).status()
        except Exception:  # noqa: BLE001
            sys.exit(f"etcd não responde em {a.etcd_host}:{a.etcd_port}. "
                     "Suba com: docker start etcd (ou docker run ... — veja o README).")

    base = a.base_port
    obs_url = f"http://localhost:{base}"
    common = os.environ.copy()
    common.update({"LOG_DIR": str(log_dir), "ETCD_HOST": a.etcd_host, "ETCD_PORT": str(a.etcd_port),
                   "OBSERVER_URL": obs_url, "PYTHONUNBUFFERED": "1"})
    out_files, daemons, clients = [], [], []

    def spawn(module, extra, name):
        env = {**common, **extra}
        out = open(log_dir / f"{name}.out", "w")
        out_files.append(out)
        stdout = None if a.verbose else out
        return subprocess.Popen([sys.executable, "-m", f"clustersync.{module}"], cwd=ROOT, env=env,
                                stdout=stdout, stderr=subprocess.STDOUT)

    try:
        daemons.append(spawn("observer", {"OBSERVER_PORT": str(base)}, "observer"))
        wait_http(f"{obs_url}/health", 20)
        for i in range(1, a.nodes + 1):
            env = build_env(a, run_id, 0)
            env.update({"NODE_ID": f"node{i}", "NODE_PORT": str(base + i)})
            daemons.append(spawn("node", env, f"node{i}"))
        for i in range(1, a.nodes + 1):
            wait_http(f"http://localhost:{base + i}/health", 60)

        start_at = time.time() + a.start_delay
        print(f"Largada conjunta em {a.start_delay:.0f}s ...", flush=True)
        for j in range(1, a.clients + 1):
            target = (j - 1) % a.nodes + 1
            env = build_env(a, run_id, start_at)
            env.update({"CLIENT_ID": f"c{j}", "NODE_URL": f"http://localhost:{base + target}"})
            clients.append(spawn("client", env, f"c{j}"))

        e = build_env(a, run_id, start_at)
        total = sum(int(e["HEAVY_REQUESTS"]) if e["HEAVY_CLIENT"] == f"c{j}" else int(e["REQUESTS"])
                    for j in range(1, a.clients + 1))
        progress_loop(clients, obs_url, total, a.verbose)
        bad = [p.returncode for p in clients if p.returncode != 0]
        if bad:
            print(f"ATENÇÃO: {len(bad)} cliente(s) terminaram com erro (veja {log_dir}/c*.out)")
    finally:
        for p in clients + daemons:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in clients + daemons:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        for f in out_files:
            f.close()


def run_docker(a, run_id: str, log_dir: Path) -> None:
    compose_file = ROOT / "docker-compose.generated.yml"
    compose_file.write_text(gen_compose.generate(a.nodes, a.clients), encoding="utf-8")
    dc = ["docker", "compose", "-f", str(compose_file)]
    env = os.environ.copy()
    env.update({"LOCAL_UID": str(os.getuid()), "LOCAL_GID": str(os.getgid())})
    env.update(build_env(a, run_id, 0))

    subprocess.run(dc + ["build"], check=True, cwd=ROOT, env=env)
    start_at = time.time() + a.start_delay
    env["START_AT"] = f"{start_at:.3f}"
    try:
        subprocess.run(dc + ["up", "-d"], check=True, cwd=ROOT, env=env)
        names = [f"client{j}" for j in range(1, a.clients + 1)]
        ids = subprocess.run(dc + ["ps", "-aq", *names], check=True, cwd=ROOT, env=env,
                             capture_output=True, text=True).stdout.split()
        print(f"Aguardando {len(ids)} clientes (acompanhe: docker compose -f {compose_file.name} logs -f) ...")
        subprocess.run(["docker", "wait", *ids], check=True, stdout=subprocess.DEVNULL)
    finally:
        if not a.keep:
            subprocess.run(dc + ["down", "-t", "10"], cwd=ROOT, env=env)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["local", "docker"], default="local")
    ap.add_argument("--scenario", choices=list(SCENARIOS), default="normal")
    ap.add_argument("--sync", choices=["etcd", "none"], default="etcd",
                    help="'none' desliga a proteção (prova que o observador detecta sobreposição)")
    ap.add_argument("--nodes", type=int, default=5, help="processos do Cluster Sync")
    ap.add_argument("--clients", type=int, default=5)
    ap.add_argument("--requests", type=int, default=10, help="pedidos por cliente (enunciado: 10 a 50)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--cs-min", type=float, default=0.2)
    ap.add_argument("--cs-max", type=float, default=1.0)
    ap.add_argument("--semaphore", type=int, default=0, metavar="K",
                    help="extensão: etapa separada com semáforo de K permissões antes de R (0 = desligada)")
    ap.add_argument("--stage-min", type=float, default=None, help="duração mínima da etapa do semáforo (padrão: --cs-min)")
    ap.add_argument("--stage-max", type=float, default=None, help="duração máxima da etapa do semáforo (padrão: --cs-max)")
    ap.add_argument("--think-min", type=float, default=None, help="sobrescreve o cenário")
    ap.add_argument("--think-max", type=float, default=None, help="sobrescreve o cenário")
    ap.add_argument("--heavy-client", default="c1", help="cliente pesado do cenário 'uneven'")
    ap.add_argument("--heavy-requests", type=int, default=None, help="padrão: min(50, 3x --requests)")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--start-delay", type=float, default=None)
    ap.add_argument("--etcd-host", default="localhost")
    ap.add_argument("--etcd-port", type=int, default=2379)
    ap.add_argument("--base-port", type=int, default=18000)
    ap.add_argument("--verbose", action="store_true", help="mostra o log de todos os processos")
    ap.add_argument("--keep", action="store_true", help="(docker) não derruba os contêineres no fim")
    a = ap.parse_args()
    if a.start_delay is None:
        a.start_delay = 3.0 if a.mode == "local" else 10.0

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    sem_tag = f"-k{a.semaphore}" if a.semaphore else ""
    run_id = a.run_id or f"{stamp}-{a.scenario}-{a.sync}{sem_tag}-n{a.nodes}-s{a.seed}"
    log_dir = ROOT / "logs" / run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    print(f"Execução {run_id}  (modo={a.mode}, nós={a.nodes}, clientes={a.clients}, sync={a.sync}"
          + (f", semáforo K={a.semaphore}" if a.semaphore else "") + ")")

    t0 = time.monotonic()
    (run_local if a.mode == "local" else run_docker)(a, run_id, log_dir)
    print(f"Execução terminou em {time.monotonic() - t0:.1f}s. Logs em {log_dir}")
    sys.exit(verify.run(log_dir))


if __name__ == "__main__":
    main()
