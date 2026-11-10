# Análise dos resultados

Interpretação das tabelas de `RESULTADOS.md` (todas do modo `--sync etcd`, modo de execução local, seção crítica de 0,2–1 s,
duração média observada E[CS] ≈ 0,60 s). Valores com 3 repetições são médias das sementes 1, 2 e 3; os demais têm 1 repetição.
Ao refazer os experimentos, atualize os números a partir do novo `RESULTADOS.md`.

## 1. A instrumentação detecta violação, e o protocolo a impede

| Execução (contenção, 5 × 5, 100 pedidos) | máx. simultâneos em R | sobreposições | vazão (acessos/s) |
|---|---|---|---|
| `--sync none` (proteção desligada) | 5 | 97 de 100 `BEGIN` | 6,93 |
| `--sync etcd` (3 sementes) | 1 | 0 | 1,61 |

Sem proteção, os 5 nós entram juntos e o observador acusa a sobreposição. Com o mutex, nenhuma sobreposição em nenhuma execução
(normal, contenção, desigual, 10 nós e com semáforo). A diferença de vazão (6,93 × 1,61) é o custo da exclusão mútua: com R
exclusivo, a vazão máxima é 1/E[CS] ≈ 1,67 acesso/s.

## 2. Carga normal × alta contenção

| | espera mediana em R | p95 | vazão | ocupação de R (vazão × E[CS]) |
|---|---|---|---|---|
| normal (pausa 1–5 s) | 0,34 s | 1,31 s | 1,19/s | ≈ 72 % |
| contenção (sem pausa) | 2,50 s | 3,15 s | 1,61/s | ≈ 97 % |

Sob contenção, R fica ocupado ≈ 97 % do tempo: a passagem de vez entre dois acessos (delete → evento de watch → releitura da fila →
`BEGIN`) custa em média 1/1,611 − 0,601 ≈ 20 ms. Como a fila é FIFO e cada cliente tem um pedido em andamento, um pedido espera as
seções críticas dos outros C − 1 clientes: (5 − 1) × 0,62 s ≈ 2,5 s, igual à mediana observada (2,50 s).

## 3. Quantidade de participantes (5 × 10)

| Configuração (contenção) | espera mediana | p95 | maior espera | vazão | ops etcd/entrada | rodadas de espera | timeouts do watch | notif./entrada |
|---|---|---|---|---|---|---|---|---|
| 5 nós × 5 clientes (3 sementes) | 2,50 s | 3,15 s | 3,84 s | 1,61/s | 5,98 | 0,99 | 0 | 0,99 |
| 5 nós × 10 clientes (1 semente) | 5,61 s | 6,51 s | 7,10 s | 1,59/s | 7,64 | 1,82 | 0,84 | 0,98 |
| 10 nós × 10 clientes (3 sementes) | 5,50 s | 6,71 s | 8,33 s | 1,61/s | 7,48 | 1,77* | 0,78* | 0,98 |

\* semente 1 (as sementes 2 e 3 não guardaram os logs completos).

* **Espera.** Cresce com o número de pedidos que disputam R, não com o número de nós: 5 nós × 10 clientes espera o mesmo que
  10 × 10. A previsão (C − 1) × 0,62 s dá 2,5 s (C = 5) e 5,6 s (C = 10), próximo do observado.
* **Vazão.** Não muda (≈ 1,6/s): R já está saturado com 5 clientes; mais participantes só aumentam a fila.
* **Notificações.** Continuam ≈ 1 por entrada com 10 participantes: cada liberação acorda só o sucessor. Se todos observassem o dono
  (ou o prefixo), cada liberação acordaria os N − 1 que esperam: ≈ 4 notificações com 5 e ≈ 9 com 10.
* **Tráfego.** O protocolo em si custa 6 chamadas por entrada com disputa, com 5 ou 10 nós. O aumento de 5,98 para 7,48 vem
  inteiramente da rede de segurança: como a espera passa de 5 s, o watch expira sem evento (0,78 timeout por entrada) e cada
  timeout custa mais 2 chamadas (releitura da fila + novo watch). Com `WATCH_POLL_S` maior, o custo voltaria a ≈ 6.

## 4. Carga desigual: justiça e prioridade

Configuração com 130 pedidos: c1 faz 50 pedidos com pausa de 0–0,3 s; c2…c5 fazem 20 com pausa de 1–5 s (1 semente).

| cliente | acessos | parcela de R | espera mediana | p95 | maior espera |
|---|---|---|---|---|---|
| c1 (pesado) | 50 | 38 % | 0,71 s | 1,93 s | 2,41 s |
| c2 | 20 | 15 % | 0,80 s | 2,31 s | 3,00 s |
| c3 | 20 | 15 % | 0,70 s | 1,55 s | 1,73 s |
| c4 | 20 | 15 % | 0,73 s | 1,35 s | 1,52 s |
| c5 | 20 | 15 % | 0,76 s | 1,64 s | 2,34 s |

* O mutex é **justo por pedido** (FIFO por `create_revision`), não por cliente: não há prioridade. O cliente pesado ocupa 38 % de R
  porque pede mais vezes, mas a espera de cada pedido dele é a mesma dos outros (medianas de 0,70 a 0,80 s para todos).
* Os clientes leves não sofrem starvation: a maior espera (3,0 s) fica abaixo do limite de (C − 1) × 1 s ≈ 4,1 s, porque nenhum pedido
  novo de c1 passa à frente de um pedido já na fila. O efeito do cliente pesado aparece só como mais concorrência: a espera mediana dos
  leves sobe de ≈ 0,34 s (carga normal) para ≈ 0,75 s, porque quase sempre há um pedido de c1 na fila.
* Se fosse preciso justiça por cliente (por exemplo, cota de acessos), seria uma política acima do mutex; o etcd só garante a ordem
  de chegada.

## 5. Extensão: semáforo distribuído (K = 2 e K = 3)

| Contenção, 5 × 5, 100 pedidos (3 sementes) | pico | ocupação média | espera mediana na etapa | espera mediana em R | vazão |
|---|---|---|---|---|---|
| sem semáforo | — | — | — | 2,50 s | 1,61/s |
| K = 2 | 2 | ≈ 1,0 (50 %) | 0,01 s | 1,85 s | 1,59/s |
| K = 3 | 3 | ≈ 1,0 (33 %) | 0,01–0,02 s | 1,81 s | 1,58/s |
| K = 3, etapa de 1–2 s (1 semente, 50 pedidos) | 3 | 2,29 (76 %) | 0,01 s (p95 0,84 s) | 1,02 s | 1,43/s |
| K = 2, `--sync none` | 5 (> K) | 2,28 | — | — | 3,65/s |

* **Limite K respeitado e uso concorrente.** O pico chega exatamente a K (as permissões são usadas em paralelo) e nunca o passa;
  sem proteção, o pico chega a 5 e o verificador acusa o excesso.
* **Por que a ocupação fica em ≈ 1 com K = 2 e K = 3.** A etapa fica em série com R, e R é o gargalo. Pela lei de Little,
  ocupação = vazão × duração média da etapa ≈ 1,6 × 0,6 ≈ 1: em média só um cliente está na etapa, então K não limita a vazão. Para
  estressar o semáforo é preciso uma etapa mais longa que R (execução com etapa de 1–2 s: ocupação de 2,29 em 3 permissões).
* **A espera em R cai com o semáforo** (2,50 → 1,8 s): em média ≈ 1 cliente está na etapa e não na fila de R, então há ≈ 3 e não 4
  pedidos à frente: (4 − 1) × 0,62 ≈ 1,9 s.
* **Custo.** Um esperando no semáforo observa as K chaves que detêm as permissões, e não só o predecessor, porque elas são liberadas fora
  de ordem. Custo por entrada na etapa: 4 + (K + 1) · rodadas de espera (K watches + 1 releitura por rodada), o que bate com o
  observado (K = 2: 4,3 com 0,1 rodada; K = 3: 4,12 com 0,03). As notificações por entrada na etapa ficaram baixas (0,10–0,16 com K = 2;
  0,03 com K = 3) porque quase ninguém precisou esperar.

## 6. Custo de mensagens: teórico × observado

Modelo do modo `etcd` (seção 2.5 do README): 4 + 2 · rodadas de espera por entrada em R.

| Configuração (semente 1) | rodadas de espera | teórico | observado | instrumentação (observador) | cliente ↔ nó |
|---|---|---|---|---|---|
| normal 5 × 5 | 0,64 | 5,28 | 5,28 | 2 | 2 |
| contenção 5 × 5 | 0,99 | 5,98 | 5,98 | 2 | 2 |
| contenção 10 × 10 | 1,77 (0,78 timeouts) | 7,54 | 7,54 | 2 | 2 |
| desigual 130 pedidos | 0,86 | 5,72 | 5,72 | 2 | 2 |
| contenção 5 × 5 com semáforo K = 2 | 0,99 | 5,98 | 5,98 | 4 (2 em R + 2 na etapa) | 2 |

* O tráfego observado bate com o modelo: sem disputa, 4 chamadas; com disputa, cada pedido espera exatamente uma liberação
  (≈ 1 rodada) e paga 6. O custo do protocolo por entrada é constante; só cresce com a rede de segurança de 5 s quando a espera é longa.
* Instrumentação e aplicação ficam fora da conta do protocolo: 2 mensagens ao observador por acesso a R (+2 na etapa do semáforo) e
  2 entre cliente e nó por pedido.
* Os keepalives da sessão não dependem da carga (1 a cada ≈ 3,3 s por nó) e não foram registrados nestes logs. Estimativa: 5 nós ×
  84 s / 3,3 s ≈ 126 keepalives para 100 entradas (≈ 1,3 por entrada) na carga normal. As próximas execuções medem esse valor
  (coluna "keepalive sessão").
* No modo `etcd-api` o nó troca só 2 mensagens de protocolo por entrada (`Lock` + `Unlock`), com ou sem disputa: a mesma receita roda
  dentro do etcd. O trabalho não some, só passa do cliente para o servidor.

## 7. Limitações

* Ausência de starvation e a espera abaixo do limite são observações de execuções curtas, coerentes com o argumento da seção 2.4 do
  README, não uma prova.
* Carga desigual e a configuração 5 × 10 têm 1 repetição; as métricas de tráfego detalhadas existem só para a semente 1 (as demais
  guardaram apenas o `summary.json` original).
* Todas as execuções usaram uma única instância do etcd e o modo local (processos na mesma máquina); sem falhas, expiração de lease
  ou replicação, conforme o enunciado.
