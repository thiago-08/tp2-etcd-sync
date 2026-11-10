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
  5b. Sessões válidas- nenhuma sessão (lease) expirou, nenhum pedido perdeu a chave,
                       toda chave foi criada no lease do próprio nó e o maior intervalo
                       entre keepalives ficou abaixo do TTL (enunciado B3: "mantendo as
                       sessões válidas durante os experimentos").
Com a extensão do semáforo (SEMAPHORE_K = K > 0), verifica também na etapa S:
  6. Limite K       - nunca mais de K acessos simultâneos (reexecução independente,
                       com K lido da configuração dos nós).
  7. Uso concorrente- as permissões foram de fato usadas em paralelo (pico >= 2).
  8. Fila da etapa  - a ordem de entrada respeita a fila por create_revision; como
                       as K permissões podem ser liberadas fora de ordem, uma entrada
                       pode ultrapassar no máximo K-1 pedidos mais antigos.
Se a execução foi feita com SYNC_MODE=none, a verificação esperada é INVERSA:
precisa detectar sobreposições (prova que a instrumentação funciona).

Tráfego (métrica "trafego_por_entrada"): separa as mensagens do PROTOCOLO (chamadas
ao etcd para acquire/release, mais os keepalives da sessão) das mensagens de
INSTRUMENTAÇÃO (BEGIN/END ao observador, leituras feitas só para o log) e da
aplicação (pedido do cliente + COMMITTED), e compara o protocolo com o custo teórico.
"""
import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

LIFECYCLE = ["REQUEST", "ACQUIRE", "ENTER", "EXIT", "RELEASE", "COMMITTED"]
SEM_LIFECYCLE = ["REQUEST", "SEM_ACQUIRE", "SEM_ENTER", "SEM_EXIT", "SEM_RELEASE",
                 "ACQUIRE", "ENTER", "EXIT", "RELEASE", "COMMITTED"]


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


def replay(events: list[dict]) -> int:
    """Reexecuta BEGIN/END (ordenados por seq) de UM recurso e devolve o pico de acessos simultâneos."""
    active, peak = set(), 0
    for r in events:
        if r["event"] == "BEGIN":
            active.add(r["request_id"])
            peak = max(peak, len(active))
        else:
            active.discard(r["request_id"])
    return peak


def over_limit(events: list[dict], limit: int) -> int:
    """Quantos BEGIN deixaram o recurso acima do limite (1 em R, K na etapa do semáforo)."""
    active, over = set(), 0
    for r in events:
        if r["event"] == "BEGIN":
            active.add(r["request_id"])
            if len(active) > limit:
                over += 1
        else:
            active.discard(r["request_id"])
    return over


def occupancy(events: list[dict]) -> float:
    """Ocupação média (permissões em uso) ponderada pelo tempo, no relógio do observador."""
    if len(events) < 2:
        return 0.0
    area, active, last = 0.0, 0, events[0]["mono"]
    for r in events:
        area += active * (r["mono"] - last)
        last = r["mono"]
        active += 1 if r["event"] == "BEGIN" else -1
    window = events[-1]["mono"] - events[0]["mono"]
    return area / window if window > 0 else 0.0


def theoretical_ops(mode: str, rounds: int) -> int:
    """Custo teórico, em chamadas ao etcd, de UMA entrada em R (acquire + release).
    etcd     : txn (entra na fila) + get (lê a própria revisão) + range (lê a fila)
               + 2 por rodada de espera (watch do predecessor + releitura da fila)
               + delete  ->  4 + 2·rodadas  (sem disputa = 4; esperando 1 liberação = 6)
    etcd-api : Lock + Unlock = 2, com ou sem disputa (a espera acontece dentro do etcd)"""
    if mode == "etcd":
        return 4 + 2 * rounds
    if mode == "etcd-api":
        return 2
    return 0


def session_check(nodes: dict) -> tuple[bool, str]:
    leases_total, expired, ka_err, errors, gaps, ttls, closed, foreign = 0, 0, 0, 0, [], [], 0, []
    for recs in nodes.values():
        leases = {r.get("lease") for r in recs if r["event"] == "SESSION_START"}
        leases_total += len(leases)
        for r in recs:
            ev = r["event"]
            if ev == "SESSION_START":
                ttls.append(r.get("ttl") or 0)
            elif ev == "SESSION_EXPIRED":
                expired += 1
            elif ev == "KEEPALIVE_ERROR":
                ka_err += 1
            elif ev == "ERROR":
                errors += 1
            elif ev == "SESSION_CLOSED":
                closed += 1
                if r.get("max_gap_s") is not None:
                    gaps.append(r["max_gap_s"])
            elif ev in ("KEY_CREATED", "LOCK_GRANTED") and r.get("lease") not in leases:
                foreign.append(r.get("request_id"))
    ttl = min(ttls) if ttls else 0
    gap_ok = not gaps or max(gaps) < ttl
    ok = leases_total >= len(nodes) > 0 and expired == 0 and errors == 0 and not foreign and gap_ok
    detail = (f"{leases_total} sessões (TTL {ttl} s) em {len(nodes)} nós, expiradas = {expired}, "
              f"pedidos com erro = {errors}, erros de keepalive = {ka_err}, chaves fora do lease do nó = {len(foreign)}, "
              f"sessões encerradas normalmente = {closed}, ")
    detail += (f"maior intervalo entre keepalives = {max(gaps):.2f} s" if gaps
               else "intervalo entre keepalives não registrado nestes logs")
    return ok, detail


def analyze(logdir: Path) -> dict:
    obs = load(logdir / "observer.jsonl") if (logdir / "observer.jsonl").exists() else []
    node_files = sorted(p for p in logdir.glob("node*.jsonl"))
    client_files = sorted(p for p in logdir.glob("*.jsonl") if re.fullmatch(r"c\d+\.jsonl", p.name))
    nodes = {p.stem: load(p) for p in node_files}
    clients = {p.stem: load(p) for p in client_files}

    sync_mode, sem_k, stage, r_name = "etcd", 0, "S", "R"
    for recs in nodes.values():
        for r in recs:
            if r["event"] == "NODE_START":
                sync_mode = r.get("sync_mode", sync_mode)
                sem_k = int(r.get("semaphore_k", 0) or 0)
                stage = r.get("stage", stage)
                r_name = r.get("resource", r_name)

    # ---- agrupa eventos dos nós por request_id
    req = defaultdict(lambda: defaultdict(list))
    for recs in nodes.values():
        for r in recs:
            if r.get("request_id"):
                req[r["request_id"]][r["event"]].append(r)

    checks = []  # (nome, ok, detalhe)

    # 1) exclusão mútua em R (reexecução independente da sequência do observador)
    by_res = defaultdict(list)
    for r in obs:
        if r["event"] in ("BEGIN", "END"):
            by_res[r.get("resource", "R")].append(r)
    for evs in by_res.values():
        evs.sort(key=lambda r: r["seq"])
    r_events = by_res.get("R", [])
    s_events = by_res.get(stage, []) if sem_k > 0 else []
    max_active = replay(r_events)
    overlaps = over_limit(r_events, 1)
    begins = [r for r in r_events if r["event"] == "BEGIN"]
    ends = [r for r in r_events if r["event"] == "END"]

    protected = sync_mode != "none"
    if protected:
        checks.append(("Exclusão mútua em R (observador)", overlaps == 0 and max_active <= 1,
                       f"máx. acessos simultâneos = {max_active}, sobreposições = {overlaps}"))
    else:
        checks.append(("Instrumentação detecta sobreposição em R (SYNC_MODE=none)", overlaps > 0,
                       f"sobreposições detectadas = {overlaps}, máx. simultâneos = {max_active}"))

    # 1b) limite K na etapa do semáforo
    s_peak = replay(s_events) if sem_k > 0 else 0
    s_over = over_limit(s_events, sem_k) if sem_k > 0 else 0
    s_begins = [r for r in s_events if r["event"] == "BEGIN"]
    s_ends = [r for r in s_events if r["event"] == "END"]
    if sem_k > 0:
        if protected:
            checks.append((f"Semáforo: nunca mais de K={sem_k} acessos simultâneos na etapa {stage}",
                           s_over == 0 and s_peak <= sem_k,
                           f"pico = {s_peak}, excessos acima de K = {s_over}"))
        else:
            checks.append((f"Instrumentação detecta excesso do limite K={sem_k} (SYNC_MODE=none)",
                           True if s_over > 0 else None,
                           f"pico = {s_peak}, excessos acima de K = {s_over}"
                           + ("" if s_over > 0 else " (carga insuficiente para exceder K)")))

    # 2) BEGIN/END (R e etapa S)
    b_ids, e_ids = {r["request_id"] for r in begins}, {r["request_id"] for r in ends}
    be_ok = b_ids == e_ids and len(begins) == len(ends)
    be_detail = f"R: BEGIN={len(begins)}, END={len(ends)}"
    if sem_k > 0:
        sb, se = {r["request_id"] for r in s_begins}, {r["request_id"] for r in s_ends}
        be_ok = be_ok and sb == se and len(s_begins) == len(s_ends)
        be_detail += f" | {stage}: BEGIN={len(s_begins)}, END={len(s_ends)}"
    checks.append(("Todo BEGIN tem END", be_ok, be_detail))

    # 3) progresso
    planned = sum(r.get("planned", 0) for recs in clients.values() for r in recs
                  if r["event"] == "CLIENT_START")
    received = sum(1 for recs in clients.values() for r in recs if r["event"] == "RECEIVED_COMMITTED")
    committed = sum(1 for d in req.values() if "COMMITTED" in d)
    checks.append(("Todos os pedidos concluídos (progresso)",
                   planned > 0 and planned == received == committed,
                   f"planejados={planned}, COMMITTED nos nós={committed}, recebidos pelos clientes={received}"))

    # 4) ciclo de vida por pedido
    lifecycle = SEM_LIFECYCLE if sem_k > 0 else LIFECYCLE
    bad = []
    for rid, d in req.items():
        if any(len(d.get(e, [])) != 1 for e in lifecycle):
            bad.append(rid)
            continue
        monos = [d[e][0]["mono"] for e in lifecycle]
        if monos != sorted(monos):
            bad.append(rid)
    checks.append(("Ciclo de vida REQUEST→…→COMMITTED em ordem" + (" (com etapa do semáforo)" if sem_k > 0 else ""), not bad,
                   "ok em todos os pedidos" if not bad else f"{len(bad)} pedidos fora de ordem: {bad[:5]}"))

    # 5) FIFO por create_revision (só faz sentido com o mutex do etcd)
    if protected:
        order = [r["request_id"] for r in begins]
        revs = [req[rid]["ACQUIRE"][0].get("create_rev") for rid in order if "ACQUIRE" in req[rid]]
        fifo_ok = len(revs) == len(order) and all(a < b for a, b in zip(revs, revs[1:]))
        checks.append(("Ordem de entrada = ordem de create_revision (FIFO)", fifo_ok,
                       f"{len(order)} entradas verificadas"))
        ok, detail = session_check(nodes)
        checks.append(("Sessões (leases) válidas durante todo o experimento", ok, detail))

    # 6/7/8) propriedades do semáforo
    sem_metrics = {}
    if sem_k > 0 and protected:
        order = [r["request_id"] for r in s_begins]
        revs = [req[rid]["SEM_ACQUIRE"][0].get("create_rev") for rid in order if "SEM_ACQUIRE" in req[rid]]
        overtakes = 0
        if len(revs) == len(order):
            overtakes = max((sum(1 for rj in revs[i + 1:] if rj < ri) for i, ri in enumerate(revs)), default=0)
        checks.append((f"Fila da etapa {stage} respeitada (ultrapassagens <= K-1 = {sem_k - 1})",
                       len(revs) == len(order) and overtakes <= sem_k - 1,
                       f"{len(order)} entradas, máx. de pedidos mais antigos ultrapassados = {overtakes}"))
        need = min(sem_k, 2)
        checks.append((f"Uso concorrente das permissões (pico >= {need})", True if s_peak >= need else None,
                       f"pico = {s_peak} de K = {sem_k}"
                       + ("" if s_peak >= need else " (carga leve: aumente a contenção)")))

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
    # ---- tráfego por entrada: protocolo x instrumentação x aplicação
    rounds, timeouts, theo, inst = [], [], [], []
    for d in req.values():
        if "ACQUIRE" not in d:
            continue
        acq = d["ACQUIRE"][0]
        if "rounds" in acq:  # logs novos
            rd, to = acq.get("rounds", 0), acq.get("timeouts", 0)
        else:  # logs antigos: conta as rodadas pelos eventos WAIT/WAKE do recurso R
            rd = sum(1 for w in d.get("WAIT", []) if w.get("resource", r_name) == r_name)
            to = sum(1 for w in d.get("WAKE", [])
                     if w.get("resource", r_name) == r_name and w.get("reason") == "timeout")
        rounds.append(rd)
        timeouts.append(to)
        theo.append(theoretical_ops(sync_mode, rd))
        inst.append(acq.get("inst_ops", 0))
    closes = [r for recs in nodes.values() for r in recs if r["event"] == "SESSION_CLOSED"]
    keepalives = (sum(r["keepalives"] for r in closes)
                  if closes and all("keepalives" in r for r in closes) else None)
    n_entries = len(rounds)
    obs_msgs = sum(len(evs) for evs in by_res.values())  # BEGIN + END de todos os recursos
    cli_msgs = sum(1 for recs in clients.values() for r in recs if r["event"] in ("SEND", "RECEIVED_COMMITTED"))

    def per_entry(x):
        return round(x / n_entries, 2) if n_entries and x is not None else None

    traffic = {
        "protocolo_R_observado": round(statistics.mean(ops), 2) if ops else 0,
        "protocolo_R_teorico": round(statistics.mean(theo), 2) if theo else 0,
        "modelo_teorico_R": {"etcd": "4 + 2·rodadas (sem disputa = 4; 1 rodada = 6)",
                             "etcd-api": "2 (Lock + Unlock; a espera ocorre dentro do etcd)"}.get(sync_mode, "0"),
        "rodadas_espera_R": round(statistics.mean(rounds), 2) if rounds else 0,
        "timeouts_watch_R": round(statistics.mean(timeouts), 2) if timeouts else 0,
        "protocolo_semaforo": None,
        "keepalives_sessao": per_entry(keepalives),
        "instrumentacao_observador": per_entry(obs_msgs),
        "instrumentacao_leituras_etcd": round(statistics.mean(inst), 2) if inst else 0,
        "aplicacao_cliente_no": per_entry(cli_msgs),
    }

    all_ev = sorted((r for evs in by_res.values() for r in evs), key=lambda r: r["seq"])
    total_s = (all_ev[-1]["mono"] - all_ev[0]["mono"]) if all_ev else 0.0  # relógio do observador

    cfg = {}
    for recs in nodes.values():
        for r in recs:
            if r["event"] == "NODE_START":
                cfg = {k: r.get(k) for k in ("sync_mode", "run_mode", "seed", "cs_min", "cs_max",
                                             "semaphore_k", "stage_min", "stage_max")}
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
    if sem_k > 0:
        sw = [d["SEM_ACQUIRE"][0]["wait_s"] for d in req.values() if "SEM_ACQUIRE" in d]
        sops = [d["SEM_ACQUIRE"][0].get("etcd_ops", 0) + d["SEM_RELEASE"][0].get("etcd_ops", 0)
                for d in req.values() if "SEM_ACQUIRE" in d and "SEM_RELEASE" in d]
        swk = [d["SEM_ACQUIRE"][0].get("wakeups", 0) for d in req.values() if "SEM_ACQUIRE" in d]
        occ = occupancy(s_events)
        sem_metrics = {
            "K": sem_k, "etapa": stage, "pico_permissoes_em_uso": s_peak,
            "ocupacao_media_permissoes": round(occ, 2),
            "utilizacao_media": f"{occ / sem_k:.0%}",
            "espera_mediana_s": round(statistics.median(sw), 3) if sw else 0,
            "espera_p95_s": round(pct(sw, 95), 3),
            "espera_max_s": round(max(sw), 3) if sw else 0,
            "operacoes_etcd_por_entrada": round(statistics.mean(sops), 2) if sops else 0,
            "notificacoes_watch_por_entrada": round(statistics.mean(swk), 2) if swk else 0,
        }
        traffic["protocolo_semaforo"] = sem_metrics["operacoes_etcd_por_entrada"]
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
        "trafego_por_entrada": traffic,
        "semaforo": sem_metrics,
    }
    return {"checks": checks, "metrics": metrics, "protected": protected}


def report(result: dict) -> int:
    print("\n=== Verificação das propriedades ===")
    ok_all = True
    for name, ok, detail in result["checks"]:
        ok_all &= ok is not False  # None = AVISO (observação, não reprova)
        tag = "AVISO" if ok is None else ("OK" if ok else "FALHOU")
        print(f"  [{tag}] {name}: {detail}")
    m = result["metrics"]
    print("\n=== Métricas ===")
    for k, v in m.items():
        if k not in ("acessos_por_cliente", "espera_mediana_por_cliente_s", "config_clientes", "config",
                     "semaforo", "trafego_por_entrada"):
            print(f"  {k:<34} {v}")
    print(f"  {'config':<34} {m['config']}")
    print(f"  {'acessos_por_cliente':<34} {m['acessos_por_cliente']}")
    print(f"  {'espera_mediana_por_cliente_s':<34} {m['espera_mediana_por_cliente_s']}")
    print("\n=== Tráfego por entrada (protocolo x instrumentação) ===")
    for k, v in m["trafego_por_entrada"].items():
        print(f"  {k:<34} {'-' if v is None else v}")
    if m.get("semaforo"):
        print("\n=== Métricas da etapa do semáforo ===")
        for k, v in m["semaforo"].items():
            print(f"  {k:<34} {v}")
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
