#!/usr/bin/env python3
"""Verificador automático: lê os logs de uma execução e checa as propriedades.

Uso:  python scripts/verify.py logs/<run_id>

Propriedades verificadas
  1. Exclusão mútua  - reexecuta a sequência BEGIN/END do observador e confere
                       que nunca houve mais de 1 acesso ativo a R.
  2. BEGIN/END       - todo BEGIN tem END correspondente.
  3. Progresso       - todos os pedidos planejados pelos clientes foram COMMITTED.
  4. Ciclo de vida   - REQUEST<ACQUIRE<ENTER<EXIT<RELEASE<COMMITTED em cada pedido
                       (comparação com o relógio monotônico do próprio nó).
  5. FIFO por revisão- a ordem de entrada na seção crítica coincide com a ordem
                       crescente de create_revision no etcd (justiça).
Se a execução foi feita com SYNC_MODE=none, a verificação esperada é INVERSA:
precisa detectar sobreposições (prova que a instrumentação funciona).
"""
import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

LIFECYCLE = ["REQUEST", "ACQUIRE", "ENTER", "EXIT", "RELEASE", "COMMITTED"]


def load(path: Path) -> list[dict]:
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


def pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(len(s) - 1, max(0, int(round(p / 100 * (len(s) - 1)))))]


def analyze(logdir: Path) -> dict:
    obs = load(logdir / "observer.jsonl") if (logdir / "observer.jsonl").exists() else []
    node_files = sorted(p for p in logdir.glob("node*.jsonl"))
    client_files = sorted(p for p in logdir.glob("*.jsonl") if re.fullmatch(r"c\d+\.jsonl", p.name))
    nodes = {p.stem: load(p) for p in node_files}
    clients = {p.stem: load(p) for p in client_files}

    sync_mode = "etcd"
    for recs in nodes.values():
        for r in recs:
            if r["event"] == "NODE_START":
                sync_mode = r.get("sync_mode", sync_mode)

    # ---- agrupa eventos dos nós por request_id
    req = defaultdict(lambda: defaultdict(list))
    for recs in nodes.values():
        for r in recs:
            if r.get("request_id"):
                req[r["request_id"]][r["event"]].append(r)

    checks = []  # (nome, ok, detalhe)

    # 1) exclusão mútua (reexecução independente da sequência do observador)
    seq_events = sorted((r for r in obs if r["event"] in ("BEGIN", "END")), key=lambda r: r["seq"])
    active, max_active, overlaps = set(), 0, 0
    for r in seq_events:
        if r["event"] == "BEGIN":
            active.add(r["request_id"])
            if len(active) > 1:
                overlaps += 1
            max_active = max(max_active, len(active))
        else:
            active.discard(r["request_id"])
    begins = [r for r in seq_events if r["event"] == "BEGIN"]
    ends = [r for r in seq_events if r["event"] == "END"]

    protected = sync_mode != "none"
    if protected:
        checks.append(("Exclusão mútua (observador)", overlaps == 0 and max_active <= 1,
                       f"máx. acessos simultâneos = {max_active}, sobreposições = {overlaps}"))
    else:
        checks.append(("Instrumentação detecta sobreposição (SYNC_MODE=none)", overlaps > 0,
                       f"sobreposições detectadas = {overlaps}, máx. simultâneos = {max_active}"))

    # 2) BEGIN/END
    b_ids, e_ids = {r["request_id"] for r in begins}, {r["request_id"] for r in ends}
    checks.append(("Todo BEGIN tem END", b_ids == e_ids and len(begins) == len(ends),
                   f"BEGIN={len(begins)}, END={len(ends)}"))

    # 3) progresso
    planned = sum(r.get("planned", 0) for recs in clients.values() for r in recs
                  if r["event"] == "CLIENT_START")
    received = sum(1 for recs in clients.values() for r in recs if r["event"] == "RECEIVED_COMMITTED")
    committed = sum(1 for d in req.values() if "COMMITTED" in d)
    checks.append(("Todos os pedidos concluídos (progresso)",
                   planned > 0 and planned == received == committed,
                   f"planejados={planned}, COMMITTED nos nós={committed}, recebidos pelos clientes={received}"))

    # 4) ciclo de vida por pedido
    bad = []
    for rid, d in req.items():
        if any(len(d.get(e, [])) != 1 for e in LIFECYCLE):
            bad.append(rid)
            continue
        monos = [d[e][0]["mono"] for e in LIFECYCLE]
        if monos != sorted(monos):
            bad.append(rid)
    checks.append(("Ciclo de vida REQUEST→…→COMMITTED em ordem", not bad,
                   "ok em todos os pedidos" if not bad else f"{len(bad)} pedidos fora de ordem: {bad[:5]}"))

    # 5) FIFO por create_revision (só faz sentido com o mutex do etcd)
    if protected:
        order = [r["request_id"] for r in begins]
        revs = [req[rid]["ACQUIRE"][0].get("create_rev") for rid in order if "ACQUIRE" in req[rid]]
        fifo_ok = len(revs) == len(order) and all(a < b for a, b in zip(revs, revs[1:]))
        checks.append(("Ordem de entrada = ordem de create_revision (FIFO)", fifo_ok,
                       f"{len(order)} entradas verificadas"))

    # ---- métricas
    waits = [d["ACQUIRE"][0]["wait_s"] for d in req.values() if "ACQUIRE" in d]
    per_client = defaultdict(list)
    for d in req.values():
        if "ACQUIRE" in d:
            per_client[d["ACQUIRE"][0]["client"]].append(d["ACQUIRE"][0]["wait_s"])
    ops = [d["ACQUIRE"][0].get("etcd_ops", 0) + d["RELEASE"][0].get("etcd_ops", 0)
           for d in req.values() if "ACQUIRE" in d and "RELEASE" in d]
    wakes = [d["ACQUIRE"][0].get("wakeups", 0) for d in req.values() if "ACQUIRE" in d]
    cs_s = [d["EXIT"][0].get("cs_s", 0) for d in req.values() if "EXIT" in d]
    total_s = (ends[-1]["mono"] - begins[0]["mono"]) if begins and ends else 0.0  # relógio do observador

    cfg = {}
    for recs in nodes.values():
        for r in recs:
            if r["event"] == "NODE_START":
                cfg = {k: r.get(k) for k in ("sync_mode", "seed", "cs_min", "cs_max")}
                break
        if cfg:
            break
    client_cfg = {}
    for recs in clients.values():
        for r in recs:
            if r["event"] == "CLIENT_START":
                client_cfg[r["component"]] = {"planned": r.get("planned"),
                                              "think": [r.get("think_min"), r.get("think_max")],
                                              "heavy": r.get("heavy")}
    metrics = {
        "config": cfg,
        "nodes": len(nodes),
        "clients": len(clients),
        "pedidos_concluidos": committed,
        "tempo_total_s": round(total_s, 3),
        "vazao_acessos_por_s": round(committed / total_s, 3) if total_s else 0,
        "espera_mediana_s": round(statistics.median(waits), 3) if waits else 0,
        "espera_media_s": round(statistics.mean(waits), 3) if waits else 0,
        "espera_p95_s": round(pct(waits, 95), 3),
        "espera_max_s": round(max(waits), 3) if waits else 0,
        "cs_media_s": round(statistics.mean(cs_s), 3) if cs_s else 0,
        "operacoes_etcd_por_entrada": round(statistics.mean(ops), 2) if ops else 0,
        "notificacoes_watch_por_entrada": round(statistics.mean(wakes), 2) if wakes else 0,
        "max_acessos_simultaneos": max_active,
        "sobreposicoes": overlaps,
        "acessos_por_cliente": {c: len(v) for c, v in sorted(per_client.items())},
        "espera_mediana_por_cliente_s": {c: round(statistics.median(v), 3) for c, v in sorted(per_client.items())},
        "config_clientes": client_cfg,
    }
    return {"checks": checks, "metrics": metrics, "protected": protected}


def report(result: dict) -> int:
    print("\n=== Verificação das propriedades ===")
    ok_all = True
    for name, ok, detail in result["checks"]:
        ok_all &= ok
        print(f"  [{'OK' if ok else 'FALHOU'}] {name}: {detail}")
    m = result["metrics"]
    print("\n=== Métricas ===")
    for k, v in m.items():
        if k not in ("acessos_por_cliente", "espera_mediana_por_cliente_s", "config_clientes", "config"):
            print(f"  {k:<34} {v}")
    print(f"  {'config':<34} {m['config']}")
    print(f"  {'acessos_por_cliente':<34} {m['acessos_por_cliente']}")
    print(f"  {'espera_mediana_por_cliente_s':<34} {m['espera_mediana_por_cliente_s']}")
    print(f"\nRESULTADO: {'APROVADO' if ok_all else 'REPROVADO'}"
          + ("" if result["protected"] else "  (execução SEM proteção: sobreposição era esperada)"))
    return 0 if ok_all else 1


def run(logdir: Path) -> int:
    result = analyze(logdir)
    (logdir / "summary.json").write_text(
        json.dumps({"checks": [{"name": n, "ok": o, "detail": d} for n, o, d in result["checks"]],
                    "metrics": result["metrics"]}, indent=2, ensure_ascii=False), encoding="utf-8")
    return report(result)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("uso: python scripts/verify.py logs/<run_id>")
    sys.exit(run(Path(sys.argv[1])))
