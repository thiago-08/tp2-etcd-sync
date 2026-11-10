#!/usr/bin/env python3
"""Reúne as execuções de logs/ em evidencias/ para a entrega.

Uso:
    python scripts/collect_evidence.py                 # agrega tudo que está em logs/
    python scripts/collect_evidence.py --full-all      # copia os logs completos de TODAS as repetições
    python scripts/collect_evidence.py --from-evidence # só regenera RESULTADOS.md a partir de evidencias/

O que faz
  * Lê logs/<run_id>/summary.json (gerado pelo verify.py ao fim de cada execução).
  * Agrupa as repetições que têm a MESMA configuração (cenário, sincronização, K, nós,
    clientes, duração da seção crítica e da etapa, pedidos) e, dentro do grupo, mantém
    a execução mais recente de cada semente.
  * Escreve evidencias/RESULTADOS.md: uma tabela por grupo, uma linha por repetição + média,
    com pedidos concluídos, tempo total, espera mediana, operações no etcd por entrada,
    acessos por cliente e, no semáforo, pico/utilização das permissões.
  * Copia para evidencias/<grupo>/: o summary.json de cada repetição e os logs
    completos (observer, nós, clientes) só da primeira repetição ("logs selecionados").
  * Marca como "fora do cenário comum" execuções com seção crítica fora de 0,2–1 s ou
    com clientes fora de 10–50 pedidos (o nome da pasta inclui a duração da seção
    crítica quando ela não é a padrão, para não misturar grupos diferentes).
"""
import argparse
import json
import math
import re
import shutil
import statistics
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CS = (0.2, 1.0)  # seção crítica do cenário comum (enunciado, seção 2)


def scenario_of(run_id: str) -> str:
    m = re.search(r"(?:^|-)(normal|contention|uneven)-", run_id)
    return m.group(1) if m else "custom"


def in_scenario(cs: tuple, clients_cfg: dict) -> bool:
    """Seção crítica dentro de 0,2–1 s e cada cliente com 10 a 50 pedidos."""
    lo, hi = cs
    cs_ok = lo is not None and hi is not None and DEFAULT_CS[0] <= lo <= hi <= DEFAULT_CS[1]
    req_ok = all(10 <= (c.get("planned") or 0) <= 50 for c in clients_cfg.values())
    return cs_ok and req_ok


def load_runs(logs_dir: Path, from_evidence: bool = False) -> list[dict]:
    runs = []
    if from_evidence:  # layout evidencias/<grupo>/semente<N>/summary.json
        dirs = sorted(p for p in logs_dir.glob("*/semente*") if p.is_dir())
    else:  # layout logs/<run_id>/summary.json (run_id começa pelo carimbo de data/hora)
        dirs = sorted(p for p in logs_dir.iterdir() if p.is_dir())
    for d in dirs:
        f = d / "summary.json"
        if not f.exists():
            continue
        s = json.loads(f.read_text(encoding="utf-8"))
        m = s["metrics"]
        cfg = m.get("config", {})
        k = int(cfg.get("semaphore_k") or 0)
        clients_cfg = m.get("config_clientes", {})
        run_id = f"{d.parent.name}/{d.name}" if from_evidence else d.name
        runs.append({
            "dir": d, "run_id": run_id, "metrics": m, "checks": s["checks"],
            "scenario": scenario_of(d.parent.name if from_evidence else d.name),
            "sync": cfg.get("sync_mode", "etcd"), "run_mode": cfg.get("run_mode"),
            "folder": d.parent.name if from_evidence else None,
            "seed": cfg.get("seed"), "k": k, "nodes": m.get("nodes"), "clients": m.get("clients"),
            "cs": (cfg.get("cs_min"), cfg.get("cs_max")),
            "stage": (cfg.get("stage_min"), cfg.get("stage_max")) if k else None,
            "planned": sum(c.get("planned") or 0 for c in clients_cfg.values()),
            "ok": all(c["ok"] is not False for c in s["checks"]),
            "warn": any(c["ok"] is None for c in s["checks"]),
            "in_scenario": in_scenario((cfg.get("cs_min"), cfg.get("cs_max")), clients_cfg),
        })
    return runs


def group_key(r: dict) -> tuple:
    return (r["scenario"], r["sync"], r["k"], r["nodes"], r["clients"], r["cs"], r["stage"], r["planned"])


def slug(r: dict) -> str:
    parts = [r["scenario"], r["sync"]]
    if tuple(r["cs"]) != DEFAULT_CS:  # senão grupos com seções críticas diferentes caem na mesma pasta
        parts.append(f"cs{r['cs'][0]}-{r['cs'][1]}")
    if r["k"]:
        parts.append(f"k{r['k']}")
        if r["stage"] != r["cs"]:
            parts.append(f"etapa{r['stage'][0]}-{r['stage'][1]}")
    parts.append(f"n{r['nodes']}x{r['clients']}")
    parts.append(f"{r['planned']}ped")
    return "-".join(str(p) for p in parts)


def load_desc(r: dict) -> str:
    cc = r["metrics"].get("config_clientes", {})
    kinds = defaultdict(list)
    for cid, c in sorted(cc.items()):
        kinds[(c.get("planned"), tuple(c.get("think") or ()))].append(cid)
    if len(kinds) == 1:
        (planned, think), _ = next(iter(kinds.items()))
        return f"cada cliente: {planned} pedidos, pausa {think[0]}–{think[1]} s"
    return "; ".join(f"{','.join(ids)}: {p} pedidos, pausa {t[0]}–{t[1]} s" for (p, t), ids in kinds.items())


def num(v) -> str:
    return f"{v:.3f}".rstrip("0").rstrip(".") if isinstance(v, float) else str(v)


def dash(v) -> str:
    return "-" if v is None or (isinstance(v, float) and math.isnan(v)) else num(v)


def mean(vals: list[float]) -> float:
    return round(statistics.mean(vals), 3) if vals else 0.0


def section(key: tuple, reps: list[dict]) -> str:
    first = reps[0]
    scenario, sync, k, nodes, clients, cs, stage, planned = key
    title = f"{scenario} | sync={sync}" + (f" | semáforo K={k}" if k else "") + f" | {nodes} nós × {clients} clientes"
    out = [f"### {title}", ""]
    if not first["in_scenario"]:
        out.append("> **Fora do cenário comum** (seção crítica fora de 0,2–1 s ou cliente fora de 10–50 pedidos): "
                   "serve só como teste e não deve ser comparada com os demais grupos.")
        out.append("")
    out.append(f"- Carga: {load_desc(first)} (total {planned} pedidos por execução)")
    out.append(f"- Seção crítica de R: {cs[0]}–{cs[1]} s" + (f" · etapa do semáforo: {stage[0]}–{stage[1]} s" if k else ""))
    seeds = ", ".join(str(r["seed"]) for r in reps)
    out.append(f"- Repetições: {len(reps)} ({'semente' if len(reps) == 1 else 'sementes'} {seeds})")
    modes = sorted({r["run_mode"] for r in reps if r.get("run_mode")})
    out.append(f"- Pasta: `evidencias/{first['folder'] or slug(first)}/`" + (f" · modo de execução: {', '.join(modes)}" if modes else ""))
    out.append("")
    hdr = ["semente", "verificação", "concluídos", "tempo total (s)", "vazão (acessos/s)",
           "espera mediana R (s)", "p95 (s)", "ops etcd/entrada", "notif. watch/entrada"]
    out += ["| " + " | ".join(hdr) + " |", "|" + "---|" * len(hdr)]
    cols = ["pedidos_concluidos", "tempo_total_s", "vazao_acessos_por_s", "espera_mediana_s",
            "espera_p95_s", "operacoes_etcd_por_entrada", "notificacoes_watch_por_entrada"]
    for r in reps:
        m = r["metrics"]
        status = "APROVADO" if r["ok"] else "REPROVADO"
        if sync == "none":
            status = "detectou sobreposição" if r["ok"] else "NÃO detectou"
        elif r["warn"]:
            status += " (com aviso)"
        out.append(f"| {r['seed']} | {status} | " + " | ".join(num(m.get(c, 0)) for c in cols) + " |")
    if len(reps) > 1:
        out.append("| **média** | | " + " | ".join(num(mean([r["metrics"].get(c, 0) for r in reps])) for c in cols) + " |")
    out.append("")
    if k:
        shdr = ["semente", "pico de permissões", "ocupação média", "utilização", "espera mediana etapa (s)",
                "p95 (s)", "ops etcd/entrada", "notif. watch/entrada"]
        out += [f"Etapa do semáforo (K={k}):", "", "| " + " | ".join(shdr) + " |", "|" + "---|" * len(shdr)]
        for r in reps:
            s = r["metrics"].get("semaforo") or {}
            out.append(f"| {r['seed']} | {s.get('pico_permissoes_em_uso', '-')} | {num(s.get('ocupacao_media_permissoes', '-'))} | "
                       f"{s.get('utilizacao_media', '-')} | {num(s.get('espera_mediana_s', '-'))} | {num(s.get('espera_p95_s', '-'))} | "
                       f"{num(s.get('operacoes_etcd_por_entrada', '-'))} | {num(s.get('notificacoes_watch_por_entrada', '-'))} |")
        out.append("")
    if any((r["metrics"].get("trafego_por_entrada") or {}) for r in reps):
        thdr = ["semente", "protocolo R observado", "protocolo R teórico", "rodadas de espera", "timeouts do watch",
                "protocolo semáforo", "keepalive sessão", "instrumentação (observador)", "cliente↔nó"]
        tcols = ["protocolo_R_observado", "protocolo_R_teorico", "rodadas_espera_R", "timeouts_watch_R",
                 "protocolo_semaforo", "keepalives_sessao", "instrumentacao_observador", "aplicacao_cliente_no"]
        model = next((r["metrics"]["trafego_por_entrada"].get("modelo_teorico_R") for r in reps
                      if r["metrics"].get("trafego_por_entrada")), "")
        out += [f"Mensagens por entrada em R — protocolo × instrumentação (modelo teórico: {model}; "
                "\"-\" = não registrado nos logs guardados desta repetição):", "",
                "| " + " | ".join(thdr) + " |", "|" + "---|" * len(thdr)]
        for r in reps:
            t = r["metrics"].get("trafego_por_entrada") or {}
            out.append(f"| {r['seed']} | " + " | ".join(dash(t.get(c)) for c in tcols) + " |")
        out.append("")
    out.append("Acessos por cliente e espera mediana por cliente (s):")
    out.append("")
    for r in reps:
        m = r["metrics"]
        acc = ", ".join(f"{c}={n}" for c, n in m.get("acessos_por_cliente", {}).items())
        wt = ", ".join(f"{c}={w}" for c, w in m.get("espera_mediana_por_cliente_s", {}).items())
        out.append(f"- semente {r['seed']}: acessos [{acc}] · espera [{wt}]")
    out.append("")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--logs", default=str(ROOT / "logs"))
    ap.add_argument("--out", default=str(ROOT / "evidencias"))
    ap.add_argument("--full-all", action="store_true", help="copia os logs completos de todas as repetições")
    ap.add_argument("--from-evidence", action="store_true",
                    help="não lê logs/: regenera RESULTADOS.md a partir dos summary.json já guardados em evidencias/")
    a = ap.parse_args()

    logs_dir, out_dir = Path(a.logs), Path(a.out)
    if a.from_evidence:
        logs_dir = out_dir
    if not logs_dir.exists():
        raise SystemExit(f"{logs_dir} não existe: rode alguma execução antes.")
    runs = load_runs(logs_dir, from_evidence=a.from_evidence)
    if not runs:
        raise SystemExit("nenhuma execução com summary.json em logs/.")

    groups = defaultdict(dict)  # chave -> {semente: execução mais recente}
    for r in runs:  # runs já está em ordem cronológica (nome começa pelo carimbo)
        groups[group_key(r)][r["seed"]] = r

    order = {"normal": 0, "contention": 1, "uneven": 2, "custom": 3}
    keys = sorted(groups, key=lambda k: (order.get(k[0], 9), k[1] == "none", k[1], k[2], k[3], k[4],
                                         str(k[5]), str(k[6]), k[7]))

    out_dir.mkdir(exist_ok=True)
    md = ["# Resultados dos experimentos", "",
          "Gerado por `scripts/collect_evidence.py` a partir de `logs/*/summary.json`. "
          "Tempos de espera medidos no próprio processo (relógio monotônico). "
          "Não compare diretamente grupos com cargas ou durações diferentes (veja o cabeçalho de cada grupo).", ""]
    copied = 0
    for key in keys:
        reps = sorted(groups[key].values(), key=lambda r: int(r["seed"] or 0))
        md.append(section(key, reps))
        if a.from_evidence:
            continue  # os arquivos já estão em evidencias/
        gdir = out_dir / slug(reps[0])
        for i, r in enumerate(reps):
            sdir = gdir / f"semente{r['seed']}"
            sdir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(r["dir"] / "summary.json", sdir / "summary.json")
            if i == 0 or a.full_all:
                for f in r["dir"].glob("*.jsonl"):
                    shutil.copy2(f, sdir / f.name)
                    copied += 1
    (out_dir / "RESULTADOS.md").write_text("\n".join(md), encoding="utf-8")

    total = sum(len(g) for g in groups.values())
    print(f"{total} execuções em {len(groups)} grupos -> {out_dir}/RESULTADOS.md")
    if a.from_evidence:
        print("RESULTADOS.md regenerado a partir de evidencias/ (nenhum arquivo copiado)")
    print(f"{copied} arquivos de log completos copiados "
          + ("(todas as repetições)" if a.full_all else "(só da primeira repetição de cada grupo)"))
    for key in keys:
        reps = groups[key]
        bad = [s for s, r in reps.items() if not r["ok"]]
        print(f"  {slug(next(iter(reps.values()))):<58} reps={len(reps)}" + (f"  REPROVADAS: sementes {bad}" if bad else ""))


if __name__ == "__main__":
    main()
