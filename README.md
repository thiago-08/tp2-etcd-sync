# TP2 — Sincronização em Sistemas Distribuídos (BCC362)

Trilha **B3 — etcd**: exclusão mútua sobre um recurso hipotético `R`, coordenada pelo mutex da
**API de concorrência do etcd** (sessão = **lease**, fila por **create_revision**, espera por **watch**),
com observador independente para verificar a propriedade. Há dois modos de sincronização (`--sync`):

| Modo | O que usa | Para quê |
|---|---|---|
| `etcd-api` | **API de concorrência do etcd**: serviço `Lock`/`Unlock` (no servidor, `concurrency.NewSession` + `concurrency.NewMutex(...).Lock()`) com a sessão (lease) de cada nó | atende literalmente o enunciado B3 ("usar a API de concorrência, sessões e mutex") |
| `etcd` (padrão) | a **mesma receita** do `concurrency.Mutex`, implementada no cliente sobre lease + transação + revisão + watch | mostra no log cada passo da disputa (chave criada, quem é o dono, quem cada um observa, liberação) |

As evidências atuais em `evidencias/` foram geradas no modo `etcd`.

> Extensão de sincronização escolhida: **semáforo distribuído (K = 2 e K = 3)** em uma etapa separada, antes da escrita em `R`.

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

Com a extensão (`--semaphore K`) o fluxo ganha uma etapa antes de `R`:
`REQUEST → [SEM_ACQUIRE → SEM_ENTER → SEM_EXIT → SEM_RELEASE] → ACQUIRE → ENTER → EXIT → RELEASE → COMMITTED`.

A sincronização (`mutex.py`, `semaphore.py`) fica separada da seção crítica simulada (`CriticalSection` em `node.py`);
no TP3 basta trocar o `sleep` pelo acesso ao Cluster Store.

## 2. Como o mutex funciona (o que explicar no seminário)

### 2.0 API de concorrência do etcd (modo `etcd-api`)

O etcd expõe a API de concorrência como serviços gRPC do próprio servidor (`v3lockpb.Lock` e `v3electionpb.Election`),
também acessíveis pelo gateway HTTP/JSON na mesma porta 2379. A biblioteca Python `etcd3` não traz o pacote
`concurrency` do cliente Go, então o nó chama o serviço `Lock` diretamente (`EtcdLockApiService` em `clustersync/mutex.py`):

```
POST /v3/lock/lock    {"name": base64("/clustersync/api-locks/R"), "lease": <lease da sessão do nó>}
                      -> bloqueia até o pedido ser o dono; responde {"key": base64(chave), "header": {"revision": ...}}
POST /v3/lock/unlock  {"key": base64(chave)}   -> apaga a chave (liberação normal)
```

No servidor, `Lock(nome, lease)` é exatamente `s := concurrency.NewSession(cli, WithLease(lease)); m := concurrency.NewMutex(s, nome); m.Lock(ctx)`.
O estado criado para cada disputa é a chave `/clustersync/api-locks/R/<lease em hex>`, presa ao lease, com seu `create_revision`
(veja ao vivo com `scripts/show_queue.py --api --watch`). Como essa chave é derivada do **lease**, dois pedidos simultâneos do mesmo nó
não podem usar a mesma sessão (teriam a mesma chave e entrariam juntos): cada pedido em andamento usa uma sessão exclusiva de um pool do nó
(com um cliente por nó, o pool tem uma única sessão). O log registra `LOCK_CALL` (pedido enviado), `LOCK_GRANTED` (chave, `create_rev`,
revisão da concessão) e `KEY_DELETED` (revisão da liberação). Como a espera acontece **dentro** do etcd, quem esperou por quem não aparece
no log deste modo — por isso existe também o modo `etcd`, abaixo, que executa a mesma receita no cliente.

### 2.1 A receita passo a passo (modo `etcd`, implementada no cliente)

A mesma receita do `concurrency.Mutex`, com log de cada passo:

1. **Sessão = lease.** Cada nó pede um *lease* (TTL 10 s) e o renova por *keepalive* a cada TTL/3. Chaves presas
   ao lease somem sozinhas se o nó morrer; no encerramento normal o lease é revogado.
2. **Entrar na fila.** O pedido cria a chave `/clustersync/locks/R/<request_id>` (presa ao lease) por uma
   **transação** `if create_revision(chave)==0 then put`. O etcd carimba a chave com um **create_revision**, número global e crescente.
3. **Quem manda é a revisão.** O dono do lock é a chave com **menor create_revision**. Ordem de chegada ao etcd = ordem da fila (FIFO).
4. **Esperar só pelo predecessor.** Quem não é o menor faz `watch` na chave do predecessor imediato (a de maior revisão
   menor que a sua), começando em `start_revision = minha revisão` para não perder um delete ocorrido entre a leitura e o watch.
   Uma liberação acorda **um** concorrente, não todos.
5. **Reavaliar antes de entrar.** Ao ser notificado (ou a cada 5 s, rede de segurança), relê a fila; só entra se for o menor.
6. **Liberar.** `delete` da chave → o sucessor é notificado. O log `KEY_DELETED` traz a `revision` da remoção e o `WAKE`
   do sucessor traz `delete_rev` = a mesma revisão: é a liberação normal "passando a vez" ao próximo da fila.

Diferença em relação ao `concurrency.Mutex`: lá a chave é `<prefixo>/<lease>` (uma por sessão); aqui é `<prefixo>/<request_id>`,
para que pedidos simultâneos do mesmo nó entrem na fila como pedidos distintos sem precisar de várias sessões.

### 2.2 Papel das revisões e das leases (nos dois modos)

* **Revisão.** Toda escrita no etcd recebe um número global e crescente (a revisão do armazenamento MVCC). O `create_revision`
  de cada chave da fila é a **senha** do pedido: define a ordem total da fila (FIFO) sem relógio físico. O watch com
  `start_revision` garante que a remoção do predecessor não se perca entre a leitura da fila e o início do watch; a revisão da
  remoção (`KEY_DELETED revision` / `WAKE delete_rev`) amarra cada liberação ao pedido que ela acordou.
* **Lease (sessão).** Cada nó abre uma sessão = lease com TTL de 10 s, renovado por keepalive a cada TTL/3 ≈ 3,3 s. Toda chave da
  fila é presa ao lease do nó: se o nó morresse, o lease expiraria e o etcd apagaria as chaves dele, sem travar a fila (falhas estão
  fora do escopo do TP e não são testadas). No encerramento normal o lease é revogado. **Sessões válidas durante o experimento:** cada
  sessão registra no `SESSION_CLOSED` o nº de keepalives e o maior intervalo entre dois keepalives; o verificador reprova a execução se
  alguma sessão expirou, se algum pedido perdeu a chave ou se esse intervalo chegou ao TTL.

Rastro real de uma disputa (campos dos logs `WAIT`/`WAKE`/`ACQUIRE`): o pedido `c1-2` entrou com revisão 19, havia 4 à frente
(dono `c4-1`, rev 13), observou o predecessor `c5-2` (rev 17), foi acordado 1 vez e entrou.

## 2.3 Extensão: semáforo distribuído (K permissões)

Cada pedido, antes de escrever em `R`, passa por uma **etapa de capacidade limitada** (simulada por um sleep) em que
no máximo **K pedidos** podem estar ao mesmo tempo, em qualquer nó. A exclusão mútua sobre `R` continua obrigatória e intacta
(o mutex só vê um pedido de cada vez, como antes). A etapa vem *antes* de `R` para que, na largada conjunta, as K permissões
sejam usadas em paralelo desde o início.

A API de concorrência do etcd não tem semáforo (só `Mutex` e `Election`), então a etapa é construída sobre as primitivas
lease + revisão + watch, nos dois modos. Receita (`clustersync/semaphore.py`), generalização do mutex (que é o caso K = 1):

1. **Entrar na fila:** o pedido cria `/clustersync/semaphores/S/<request_id>` presa ao lease do nó (mesma transação idempotente do mutex).
2. **Regra de entrada:** o pedido **tem permissão** quando existem **menos de K chaves com `create_revision` menor** que a dele,
   isto é, quando está entre os K mais antigos da fila.
3. **Esperar:** senão, observa (`watch`) as **K chaves que hoje detêm as permissões** e, quando **qualquer uma** é apagada,
   **reavalia a fila** antes de entrar.
4. **Liberar:** `delete` da própria chave.

**Por que observar K chaves e não só o predecessor, como no mutex?** No mutex só o dono apaga a chave, então o predecessor
imediato é sempre o próximo a sair. No semáforo as K permissões são liberadas **em qualquer ordem**: se um esperando observasse
um predecessor específico, uma permissão ficaria ociosa enquanto ele ainda estivesse em uso. Custo: uma liberação pode acordar
mais de um esperando (métrica `notificacoes_watch_por_entrada` da etapa) — no mutex é ~1.

**Por que nunca passa de K (argumento de segurança):** a chave de um pedido existe da entrada na fila até o `release`. Se K+1 pedidos
estivessem dentro ao mesmo tempo, o de maior revisão teria visto as outras K chaves à sua frente na leitura da fila (já criadas e ainda
não liberadas) e não teria entrado.

**Justiça:** a fila é por `create_revision`, mas os K que recebem permissão no mesmo instante registram o `BEGIN` numa corrida; por isso
o verificador aceita que uma entrada ultrapasse **no máximo K−1** pedidos mais antigos (K = 1 recai no FIFO estrito do mutex).

Teste de estresse isolado do semáforo (4 nós × 3 threads, liberação em ordem aleatória, contador independente): K = 1, 2, 3, 5
→ 72/72 acessos, pico = K, 0 violações, 0 travamentos.

## 2.4 Garantias: exclusão mútua, progresso, deadlock e starvation

Hipóteses do enunciado: sem quedas de processos nem perda de mensagens; etcd em uma instância (leituras linearizáveis).

* **Exclusão mútua.** A chave de um pedido existe do momento em que ele entra na fila até o seu `release`. Se dois pedidos A e B
  (rev. A < rev. B) estivessem em `R` ao mesmo tempo, B teria lido a fila depois de criar a própria chave, quando a chave de A já
  existia e ainda não tinha sido apagada; B veria A à frente e não entraria. Contradição. (Semáforo: mesmo argumento com K+1 pedidos, seção 2.3.)
* **Progresso.** Se `R` está livre e há pedidos, o de menor revisão não tem ninguém à frente: na próxima leitura da fila ele entra.
  Ele nunca dorme para sempre: o watch começa em `start_revision` (não perde a remoção do predecessor) e, por segurança, a fila é
  relida a cada 5 s mesmo sem evento.
* **Deadlock.** Cada pedido espera somente por um pedido de revisão **menor** (o predecessor). O grafo "espera por" segue a ordem
  estrita das revisões, logo não tem ciclo. O semáforo e `R` são obtidos em sequência e a permissão da etapa é devolvida antes de
  pedir `R`: ninguém segura um recurso enquanto espera o outro.
* **Starvation (espera limitada).** Pedidos novos recebem revisões maiores e entram atrás: ninguém ultrapassa um pedido já na fila.
  Cada cliente tem no máximo um pedido em andamento, então um pedido espera no máximo C−1 seções críticas (C = nº de clientes):
  ≤ 4 × 1 s com 5 clientes e ≤ 9 × 1 s com 10. Nas evidências a maior espera foi 3,84 s (5 clientes) e 8,33 s (10 clientes),
  dentro do limite. Isso é uma observação experimental que confirma o argumento, não uma prova; o argumento depende da hipótese de
  ausência de falhas.

## 2.5 Custo de mensagens: teórico × observado

Mensagens do **protocolo** (chamadas ao etcd) por entrada em `R`, separadas das mensagens de **instrumentação** (`BEGIN`/`END` ao
observador e leituras feitas só para o log) e da **aplicação** (pedido do cliente + `COMMITTED`). O verificador calcula as três
(métrica `trafego_por_entrada`) e o `RESULTADOS.md` as mostra lado a lado.

| Modo | Sem disputa | Esperando 1 liberação | Cada rodada extra (timeout de 5 s sem evento) |
|---|---|---|---|
| `etcd` (receita no cliente) | 4: `txn` + `get` + leitura da fila + `delete` | 6: + `watch` do predecessor + releitura da fila | +2 |
| `etcd-api` (serviço Lock) | 2: `Lock` + `Unlock` | 2 (a espera acontece dentro do etcd) | 0 |
| `concurrency.Mutex` do cliente Go, para comparação | 2: uma `txn` (cria a chave e lê o dono) + `delete` | a espera acrescenta leituras e um watch por predecessor | — |

Mais, em todos os modos: keepalive da sessão (1 a cada ≈ 3,3 s por nó, independente da carga) · instrumentação: 2 por entrada em `R`
(+2 na etapa do semáforo) · aplicação: 2 por pedido.

Notificações de watch: cada liberação acorda **só o sucessor** (≈ 1 notificação por entrada sob contenção, observado 0,99 com 5 e com
10 nós). Se todos observassem o dono ou o prefixo inteiro, cada liberação acordaria os N−1 que esperam (efeito manada).

## 3. Requisitos e versões

| Componente | Versão |
|---|---|
| Windows + WSL2 (Ubuntu) | — |
| Docker Desktop (integração WSL ativada) | — |
| Python | 3.12 |
| etcd (imagem `quay.io/coreos/etcd`) | v3.5.17 (documentação da API: https://etcd.io/docs/v3.5/learning/api/ e https://etcd.io/docs/v3.5/dev-guide/api_concurrency_reference_v3/) |
| etcd3 (cliente Python) / grpcio / protobuf | 0.12.0 / 1.85.1 / 3.20.3 |

> `protobuf==3.20.3` é necessário: o `etcd3` é antigo e quebra com protobuf novo (`Descriptors cannot be created directly`).

## 4. Instalação

```bash
git clone <url-do-repositorio> && cd tp2-etcd-sync
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

`docker-compose.generated.yml` (gerado a cada execução em modo docker) já está no `.gitignore`.

## 5. Execução

### 5.1 Modo local (desenvolvimento: processos Python na sua máquina, etcd no Docker)

```bash
# primeira vez: cria o contêiner do etcd (mesma versão do docker-compose)
docker run -d --name etcd -p 2379:2379 quay.io/coreos/etcd:v3.5.17 \
  etcd --name s1 --data-dir /etcd-data \
  --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://localhost:2379
# nas próximas vezes
docker start etcd
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
`docker-compose.yml` (5 nós × 5 clientes) está versionado para quem preferir rodar à mão: `docker compose build` e depois
`docker compose up`. A imagem da aplicação é construída uma única vez (só o serviço `observer` tem `build`); os demais
serviços reutilizam essa imagem (`pull_policy: never`), por isso o `build` precisa vir antes do `up`.
Porta 2379 ocupada (`port is already allocated`)? O contêiner `etcd` do modo local está ligado: `docker stop etcd`, ou rode com
`ETCD_HOST_PORT=23790` para expor o etcd do compose em outra porta.

### 5.3 Cenários do enunciado

| Experimento | Comando |
|---|---|
| Carga normal | `--scenario normal --requests 10` |
| Alta contenção (sem espera de 1–5 s) | `--scenario contention --requests 20` |
| Carga desigual (c1 pesado: 3× pedidos, pausa 0–0,3 s) | `--scenario uneven --heavy-client c1` |
| **Proteção desligada** (observador deve acusar sobreposição) | `--scenario contention --sync none` |
| **API de concorrência do etcd** (Lock/Unlock) em vez da receita no cliente | acrescente `--sync etcd-api` a qualquer cenário |
| 3 repetições com a API de concorrência | `scripts/run_series.sh normal --requests 20 --sync etcd-api` e `scripts/run_series.sh contention --requests 20 --sync etcd-api` |
| **Semáforo K=2** (etapa separada antes de R) | `--scenario contention --semaphore 2` |
| **Semáforo K=3** | `--scenario contention --semaphore 3` |
| Semáforo saturado (etapa mais longa que R → permissões sempre ocupadas) | `--semaphore 3 --stage-min 1 --stage-max 2` |
| Semáforo sem proteção (deve detectar excesso de K) | `--semaphore 2 --sync none` |
| 3 repetições do semáforo | `scripts/run_series.sh contention --semaphore 2 --requests 20` |
| 5 × 10 processos do Cluster Sync | `--nodes 5 --clients 5` e `--nodes 10 --clients 10` |
| 3 repetições (sementes 1, 2, 3) | `scripts/run_series.sh contention --requests 20` |

Com 10 nós use também `--clients 10`; do contrário metade dos nós fica sem cliente.
Não compare execuções com `--cs-min/--cs-max` ou cargas diferentes. O enunciado fixa a seção crítica em 0,2–1 s e 10 a 50 pedidos
por cliente: o `run_experiment.py` avisa e o `RESULTADOS.md` marca como "fora do cenário comum" as execuções fora disso.

### 5.4 Ver a fila do lock ao vivo (para o seminário)

Em outro terminal, enquanto um experimento roda:

```bash
python scripts/show_queue.py --watch          # modo etcd     (chaves /clustersync/locks/R/<request_id>)
python scripts/show_queue.py --api --watch    # modo etcd-api (chaves /clustersync/api-locks/R/<lease>)
# ou direto no etcd (modo local, contêiner "etcd"):
docker exec etcd etcdctl get --prefix /clustersync/locks/R/ -w fields | grep -E "Key|CreateRevision"
# modo docker (contêiner criado pelo compose):
docker compose -f docker-compose.generated.yml exec etcd etcdctl get --prefix /clustersync/ -w fields | grep -E "Key|CreateRevision"
```

## 6. Logs e verificação

* `logs/<run_id>/observer.jsonl` — `BEGIN`/`END` com `seq` (ordem global de chegada), `active`, `overlap`.
* `logs/<run_id>/node*.jsonl` — `REQUEST, KEY_CREATED, WAIT, WAKE, ACQUIRE, ENTER, EXIT, KEY_DELETED, RELEASE, COMMITTED`
  com `request_id`, `create_rev`, dono atual, predecessor observado, revisão da liberação (`KEY_DELETED revision`) e do evento que
  acordou o sucessor (`WAKE delete_rev`); no modo `etcd-api`, `LOCK_CALL`/`LOCK_GRANTED` no lugar de `KEY_CREATED`/`WAIT`/`WAKE`.
  `SESSION_START`/`SESSION_CLOSED` registram o lease, o TTL, os keepalives e o maior intervalo entre eles.
* `logs/<run_id>/c*.jsonl` — `SEND` / `RECEIVED_COMMITTED` com latência.
* `logs/<run_id>/summary.json` — resultado do verificador.

* `SEM_ACQUIRE, SEM_ENTER, SEM_EXIT, SEM_RELEASE` (nós) e `BEGIN/END` com `resource=S` e `limit=K` (observador), quando há semáforo.

`python scripts/verify.py logs/<run_id>` confere: exclusão mútua (reexecutando a sequência do observador), todo `BEGIN` com `END`,
todos os pedidos concluídos, ciclo de vida em ordem, **FIFO por create_revision** e **sessões válidas durante todo o experimento**
(nenhum lease expirado, nenhuma chave fora do lease do nó, intervalo entre keepalives < TTL). Calcula também tempo total, espera
mediana/média/p95/máx. (relógio **monotônico do próprio nó**), acessos por cliente, operações ao etcd e notificações de watch por entrada,
e o tráfego por entrada separado em protocolo (observado × teórico, seção 2.5), keepalive, instrumentação e aplicação.
Com `--semaphore K` confere ainda: **nunca mais de K acessos simultâneos** na etapa (K lido da configuração dos nós, reexecutando a
sequência do observador), **uso concorrente das permissões** (pico ≥ 2), fila da etapa respeitada (ultrapassagens ≤ K−1) e
reporta pico, ocupação média ponderada pelo tempo e espera na etapa. `[AVISO]` = observação que não reprova (ex.: carga leve demais
para exercitar as permissões). Em execuções com `--sync none` a verificação é invertida: precisa detectar sobreposição / excesso de K.

## 6.1 Evidências para a entrega (`evidencias/`)

O enunciado pede "logs selecionados" (não todos) e métricas de pelo menos **3 repetições** da carga normal e da alta contenção.
Depois de rodar os experimentos (a pasta `logs/` é local e ignorada pelo git):

```bash
python scripts/collect_evidence.py             # gera evidencias/RESULTADOS.md e copia os logs selecionados
python scripts/collect_evidence.py --full-all  # (opcional) logs completos de todas as repetições
python scripts/collect_evidence.py --from-evidence  # só regenera RESULTADOS.md com o que já está em evidencias/
git add evidencias && git commit -m "Evidências dos experimentos"
```

* `evidencias/ANALISE.md`: interpretação dos resultados (justiça, 5 × 10 participantes, semáforo, custo de mensagens) — base para os slides.
* `evidencias/RESULTADOS.md`: uma tabela por configuração (cenário, sincronização, K, nós × clientes, durações), uma linha por
  repetição + média: pedidos concluídos, tempo total, vazão, espera mediana/p95, operações no etcd e notificações de watch por entrada,
  acessos e espera por cliente e, no semáforo, pico/utilização das permissões.
* `evidencias/<configuração>/semente<N>/summary.json`: resultado do verificador de cada repetição.
* Logs completos (`observer`, `node*`, `c*`): só da primeira repetição de cada configuração (≈ 0,7–1 MB por execução de 100 pedidos).
* Execuções da mesma configuração e semente: vale a mais recente. Grupos com cargas diferentes ficam separados — não os compare diretamente.
* Guarde também os comandos usados (ex.: `evidencias/COMANDOS.md`) para reprodutibilidade.

## 7. Estrutura

```
clustersync/   mutex.py (receita etcd) · semaphore.py (extensão) · node.py · client.py · observer.py · logutil.py · http_util.py · config.py
scripts/       run_experiment.py · run_series.sh · verify.py · collect_evidence.py · show_queue.py · gen_compose.py
Dockerfile · docker-compose.yml · requirements.txt
logs/ (ignorado pelo git)   evidencias/ (logs selecionados para a entrega)
```

## 8. Hipóteses e limites (conforme o enunciado)

Sem quedas de processos, sem perda de mensagens, sem testes de expiração de lease, eleição ou replicação; etcd em instância única.
`COMMITTED` significa fim do acesso simulado, não persistência. Ausência de starvation numa execução curta é observação, não prova.

## 9. Pendências do grupo

- [x] Extensão implementada: **semáforo distribuído** (K = 2 e K = 3)
- [x] Semáforo com K=2 e K=3 em carga normal e alta contenção (3 repetições) em `evidencias/`
- [x] 3 repetições de *normal* e de *contention*; logs selecionados em `evidencias/`
- [x] 5 × 10 processos e carga desigual executados; discussão de justiça em `evidencias/ANALISE.md`
- [ ] Rodar `--sync etcd-api` (API de concorrência) em *normal* e *contention* (3 repetições) e coletar as evidências
- [ ] Rodar ao menos uma execução em `--mode docker` (as evidências atuais são do modo local) e guardá-la em `evidencias/`
- [ ] Repetir *uneven* com 3 sementes (hoje há 1 repetição por configuração)
- [ ] Versionar o script do teste de estresse do semáforo citado na seção 2.3 (ou retirar a menção)
- [ ] Guardar os comandos usados em `evidencias/COMANDOS.md`
- [ ] Slides (10–15) e ensaio do seminário (25 min)
