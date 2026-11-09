# TP2 — Sincronização em Sistemas Distribuídos (BCC362)

Trilha **B3 — etcd**: exclusão mútua sobre um recurso hipotético `R`, coordenada por um
mutex distribuído construído sobre as primitivas do etcd (**lease**, **create_revision**,
**transação** e **watch**), com observador independente para verificar a propriedade.

> Extensão de sincronização escolhida: **(a definir)** — semáforo / barreira / leitores-escritores / múltiplos recursos.

## 1. Arquitetura

```
 c1 ─► node1 ─┐                         ┌──────────────┐
 c2 ─► node2 ─┤   acquire / release     │     etcd     │  fila de R = chaves
 c3 ─► node3 ─┼────────────────────────►│ (lease, txn, │  /clustersync/locks/R/<request_id>
 c4 ─► node4 ─┤                         │  watch, rev) │  ordenadas por create_revision
 c5 ─► node5 ─┘                         └──────────────┘
      │ │ │ │ │   BEGIN / END (só registro, nunca autoriza)
      └─┴─┴─┴─┴──────────────────► observer (conta acessos ativos a R e acusa sobreposição)
```

* **Cluster Sync** = `node1..nodeN` (processos independentes, HTTP). **Cada cliente conhece um único nó.**
* O **etcd é o serviço de coordenação** (implantação mínima, 1 instância); os nós **não** se falam diretamente.
* O **observador** não concede permissão nem bloqueia `BEGIN`: só registra e acusa sobreposição.
* `request_id` único = `<cliente>-<n>` (ex.: `c3-7`). Timestamp físico aparece nos logs apenas como informação.

Fluxo de um pedido (`clustersync/node.py`):
`REQUEST → acquire(R, id) → ACQUIRE → BEGIN(observador) → ENTER → sleep 0,2–1 s → END(observador) → EXIT → release(R, id) → RELEASE → COMMITTED`.

A sincronização (`mutex.py`) fica separada da seção crítica simulada (`CriticalSection` em `node.py`);
no TP3 basta trocar o `sleep` pelo acesso ao Cluster Store.

## 2. Como o mutex funciona (o que explicar no seminário)

A biblioteca Python `etcd3` não traz o `concurrency.Mutex` do cliente Go, então implementamos a mesma receita:

1. **Sessão = lease.** Cada nó pede um *lease* (TTL 10 s) e o renova por *keepalive* a cada TTL/3. Chaves presas
   ao lease somem sozinhas se o nó morrer; no encerramento normal o lease é revogado.
2. **Entrar na fila.** O pedido cria a chave `/clustersync/locks/R/<request_id>` (presa ao lease) por uma
   **transação** `if create_revision(chave)==0 then put`. O etcd carimba a chave com um **create_revision**, número global e crescente.
3. **Quem manda é a revisão.** O dono do lock é a chave com **menor create_revision**. Ordem de chegada ao etcd = ordem da fila (FIFO).
4. **Esperar só pelo predecessor.** Quem não é o menor faz `watch` na chave do predecessor imediato (a de maior revisão
   menor que a sua), começando em `start_revision = minha revisão` para não perder um delete ocorrido entre a leitura e o watch.
   Uma liberação acorda **um** concorrente, não todos.
5. **Reavaliar antes de entrar.** Ao ser notificado (ou a cada 5 s, rede de segurança), relê a fila; só entra se for o menor.
6. **Liberar.** `delete` da chave → o sucessor é notificado.

Rastro real de uma disputa (campos dos logs `WAIT`/`WAKE`/`ACQUIRE`): o pedido `c1-2` entrou com revisão 19, havia 4 à frente
(dono `c4-1`, rev 13), observou o predecessor `c5-2` (rev 17), foi acordado 1 vez e entrou.

## 3. Requisitos e versões

| Componente | Versão |
|---|---|
| Windows + WSL2 (Ubuntu) | — |
| Docker Desktop (integração WSL ativada) | — |
| Python | 3.12 |
| etcd (imagem `quay.io/coreos/etcd`) | v3.5.17 |
| etcd3 (cliente Python) / grpcio / protobuf | 0.12.0 / 1.85.1 / 3.20.3 |

> `protobuf==3.20.3` é necessário: o `etcd3` é antigo e quebra com protobuf novo (`Descriptors cannot be created directly`).

## 4. Instalação

```bash
git clone <url-do-repositorio> && cd tp2-etcd-sync
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
echo "docker-compose.generated.yml" >> .gitignore    # arquivo gerado a cada execução em modo docker
```

## 5. Execução

### 5.1 Modo local (desenvolvimento: processos Python na sua máquina, etcd no Docker)

```bash
docker start etcd        # ou crie o contêiner conforme o passo de preparação; porta 2379
python scripts/run_experiment.py --scenario normal --requests 10
```

O script sobe observador + nós + clientes, espera terminar, desliga tudo e roda o verificador.
Logs em `logs/<run_id>/`. Para testes rápidos: `--cs-min 0.05 --cs-max 0.15 --think-min 0.1 --think-max 0.3`.

### 5.2 Modo Docker (um contêiner por processo; é o modo para a demonstração)

```bash
docker rm -f etcd        # libera a porta 2379 se você criou o contêiner manualmente
python scripts/run_experiment.py --mode docker --scenario normal --requests 10
```

Gera `docker-compose.generated.yml`, constrói a imagem, sobe tudo, espera os clientes e derruba (`--keep` mantém).
`docker-compose.yml` (5 nós × 5 clientes) está versionado para quem preferir `docker compose up` manual.

### 5.3 Cenários do enunciado

| Experimento | Comando |
|---|---|
| Carga normal | `--scenario normal --requests 10` |
| Alta contenção (sem espera de 1–5 s) | `--scenario contention --requests 20` |
| Carga desigual (c1 pesado: 3× pedidos, pausa 0–0,3 s) | `--scenario uneven --heavy-client c1` |
| **Proteção desligada** (observador deve acusar sobreposição) | `--scenario contention --sync none` |
| 5 × 10 processos do Cluster Sync | `--nodes 5 --clients 5` e `--nodes 10 --clients 10` |
| 3 repetições (sementes 1, 2, 3) | `scripts/run_series.sh contention --requests 20` |

Com 10 nós use também `--clients 10`; do contrário metade dos nós fica sem cliente.
Não compare execuções com `--cs-min/--cs-max` ou cargas diferentes.

### 5.4 Ver a fila do lock ao vivo (para o seminário)

Em outro terminal, enquanto um experimento roda:

```bash
python scripts/show_queue.py --watch
# ou direto no etcd:
docker exec etcd etcdctl get --prefix /clustersync/locks/R/ -w fields | grep -E "Key|CreateRevision"
```

## 6. Logs e verificação

* `logs/<run_id>/observer.jsonl` — `BEGIN`/`END` com `seq` (ordem global de chegada), `active`, `overlap`.
* `logs/<run_id>/node*.jsonl` — `REQUEST, KEY_CREATED, WAIT, WAKE, ACQUIRE, ENTER, EXIT, KEY_DELETED, RELEASE, COMMITTED`
  com `request_id`, `create_rev`, dono atual e predecessor observado.
* `logs/<run_id>/c*.jsonl` — `SEND` / `RECEIVED_COMMITTED` com latência.
* `logs/<run_id>/summary.json` — resultado do verificador.

`python scripts/verify.py logs/<run_id>` confere: exclusão mútua (reexecutando a sequência do observador), todo `BEGIN` com `END`,
todos os pedidos concluídos, ciclo de vida em ordem e **FIFO por create_revision**. Calcula também tempo total, espera
mediana/média/p95/máx. (relógio **monotônico do próprio nó**), acessos por cliente, operações ao etcd e notificações de watch por entrada.
Em execuções com `--sync none` a verificação é invertida: precisa detectar sobreposição.

## 7. Estrutura

```
clustersync/   mutex.py (receita etcd) · node.py · client.py · observer.py · logutil.py · http_util.py · config.py
scripts/       run_experiment.py · run_series.sh · verify.py · show_queue.py · gen_compose.py
Dockerfile · docker-compose.yml · requirements.txt
logs/ (ignorado pelo git)   evidencias/ (logs selecionados para a entrega)
```

## 8. Hipóteses e limites (conforme o enunciado)

Sem quedas de processos, sem perda de mensagens, sem testes de expiração de lease, eleição ou replicação; etcd em instância única.
`COMMITTED` significa fim do acesso simulado, não persistência. Ausência de starvation numa execução curta é observação, não prova.

## 9. Pendências do grup

- [ ] Definir e implementar a **extensão** (reutiliza `mutex.py` e o observador)
- [ ] Rodar as 3 repetições de *normal* e de *contention*; copiar logs selecionados para `evidencias/`
- [ ] Comparar 5 × 10 processos, carga desigual e discutir justiça
- [ ] Slides (10–15) e ensaio do seminário (25 min)
