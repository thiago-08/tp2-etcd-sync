# Resultados dos experimentos

Gerado por `scripts/collect_evidence.py` a partir de `logs/*/summary.json`. Tempos de espera medidos no próprio processo (relógio monotônico). Não compare diretamente grupos com cargas ou durações diferentes (veja o cabeçalho de cada grupo).

### normal | sync=etcd | 5 nós × 5 clientes

- Carga: cada cliente: 10 pedidos, pausa 1.0–5.0 s (total 50 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s
- Repetições: 1 (semente 1)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 50 | 39.964 | 1.251 | 0.307 | 2.064 | 5.16 | 0.58 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=10, c2=10, c3=10, c4=10, c5=10] · espera [c1=0.35, c2=0.261, c3=0.51, c4=0.158, c5=0.026]

### normal | sync=etcd | 5 nós × 5 clientes

- Carga: cada cliente: 20 pedidos, pausa 1.0–5.0 s (total 100 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s
- Repetições: 3 (sementes 1, 2, 3)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 100 | 82.797 | 1.208 | 0.331 | 1.406 | 5.28 | 0.64 |
| 2 | APROVADO | 100 | 81.019 | 1.234 | 0.355 | 1.23 | 5.32 | 0.66 |
| 3 | APROVADO | 100 | 88.302 | 1.132 | 0.333 | 1.278 | 5.28 | 0.64 |
| **média** | | 100 | 84.039 | 1.191 | 0.34 | 1.305 | 5.293 | 0.647 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=0.301, c2=0.622, c3=0.477, c4=0.253, c5=0.038]
- semente 2: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=0.43, c2=0.407, c3=0.304, c4=0.131, c5=0.393]
- semente 3: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=0.43, c2=0.37, c3=0.352, c4=0.47, c5=0.044]

### normal | sync=etcd | semáforo K=2 | 5 nós × 5 clientes

- Carga: cada cliente: 20 pedidos, pausa 1.0–5.0 s (total 100 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s · etapa do semáforo: 0.2–1.0 s
- Repetições: 3 (sementes 1, 2, 3)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 100 | 93.297 | 1.072 | 0.097 | 1.088 | 5.16 | 0.58 |
| 2 | APROVADO | 100 | 94.852 | 1.054 | 0.115 | 1.172 | 5.08 | 0.54 |
| 3 | APROVADO | 100 | 101.954 | 0.981 | 0.019 | 0.98 | 4.92 | 0.46 |
| **média** | | 100 | 96.701 | 1.036 | 0.077 | 1.08 | 5.053 | 0.527 |

Etapa do semáforo (K=2):

| semente | pico de permissões | ocupação média | utilização | espera mediana etapa (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|
| 1 | 2 | 0.65 | 33% | 0.013 | 0.266 | 4.45 | 0.15 |
| 2 | 2 | 0.65 | 33% | 0.014 | 0.501 | 4.42 | 0.14 |
| 3 | 2 | 0.61 | 30% | 0.013 | 0.417 | 4.39 | 0.13 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=0.074, c2=0.015, c3=0.089, c4=0.179, c5=0.219]
- semente 2: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=0.286, c2=0.022, c3=0.277, c4=0.329, c5=0.062]
- semente 3: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=0.015, c2=0.017, c3=0.02, c4=0.085, c5=0.018]

### normal | sync=etcd | semáforo K=3 | 5 nós × 5 clientes

- Carga: cada cliente: 20 pedidos, pausa 1.0–5.0 s (total 100 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s · etapa do semáforo: 0.2–1.0 s
- Repetições: 3 (sementes 1, 2, 3)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 100 | 90.041 | 1.111 | 0.054 | 1.057 | 5 | 0.5 |
| 2 | APROVADO | 100 | 94.983 | 1.053 | 0.191 | 1.088 | 5.08 | 0.54 |
| 3 | APROVADO | 100 | 99.319 | 1.007 | 0.076 | 0.851 | 5.04 | 0.52 |
| **média** | | 100 | 94.781 | 1.057 | 0.107 | 0.999 | 5.04 | 0.52 |

Etapa do semáforo (K=3):

| semente | pico de permissões | ocupação média | utilização | espera mediana etapa (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|
| 1 | 3 | 0.67 | 22% | 0.012 | 0.024 | 4.12 | 0.03 |
| 2 | 3 | 0.65 | 22% | 0.015 | 0.24 | 4.12 | 0.03 |
| 3 | 3 | 0.62 | 21% | 0.012 | 0.039 | 4.16 | 0.04 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=0.094, c2=0.022, c3=0.012, c4=0.175, c5=0.085]
- semente 2: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=0.218, c2=0.104, c3=0.345, c4=0.127, c5=0.204]
- semente 3: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=0.185, c2=0.015, c3=0.016, c4=0.223, c5=0.021]

### contention | sync=etcd | 5 nós × 5 clientes

- Carga: cada cliente: 10 pedidos, pausa 0.0–0.0 s (total 50 pedidos por execução)
- Seção crítica de R: 0.05–0.15 s
- Repetições: 1 (semente 1)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 50 | 6.201 | 8.063 | 0.474 | 0.606 | 5.96 | 0.98 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=10, c2=10, c3=10, c4=10, c5=10] · espera [c1=0.474, c2=0.517, c3=0.474, c4=0.479, c5=0.467]

### contention | sync=etcd | 5 nós × 10 clientes

- Carga: cada cliente: 10 pedidos, pausa 0.0–0.0 s (total 100 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s
- Repetições: 1 (semente 1)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 100 | 62.753 | 1.594 | 5.613 | 6.511 | 7.64 | 0.98 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=10, c10=10, c2=10, c3=10, c4=10, c5=10, c6=10, c7=10, c8=10, c9=10] · espera [c1=5.651, c10=5.395, c2=5.767, c3=5.661, c4=5.612, c5=5.516, c6=5.717, c7=5.915, c8=5.454, c9=5.385]

### contention | sync=etcd | 5 nós × 5 clientes

- Carga: cada cliente: 20 pedidos, pausa 0.0–0.0 s (total 100 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s
- Repetições: 3 (sementes 1, 2, 3)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 100 | 63.078 | 1.585 | 2.509 | 3.145 | 5.98 | 0.99 |
| 2 | APROVADO | 100 | 62.158 | 1.609 | 2.503 | 3.135 | 5.98 | 0.99 |
| 3 | APROVADO | 100 | 61.032 | 1.638 | 2.491 | 3.17 | 5.98 | 0.99 |
| **média** | | 100 | 62.089 | 1.611 | 2.501 | 3.15 | 5.98 | 0.99 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=2.51, c2=2.589, c3=2.547, c4=2.396, c5=2.27]
- semente 2: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=2.538, c2=2.376, c3=2.535, c4=2.569, c5=2.477]
- semente 3: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=2.263, c2=2.46, c3=2.424, c4=2.491, c5=2.523]

### contention | sync=etcd | 5 nós × 5 clientes

- Carga: cada cliente: 10 pedidos, pausa 0.0–0.0 s (total 50 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s
- Repetições: 1 (semente 1)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 50 | 32.57 | 1.535 | 2.504 | 3.372 | 5.96 | 0.98 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=10, c2=10, c3=10, c4=10, c5=10] · espera [c1=2.453, c2=2.831, c3=2.471, c4=2.486, c5=2.541]

### contention | sync=etcd | 10 nós × 10 clientes

- Carga: cada cliente: 20 pedidos, pausa 0.0–0.0 s (total 200 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s
- Repetições: 3 (sementes 1, 2, 3)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 200 | 124.253 | 1.61 | 5.49 | 6.705 | 7.54 | 0.99 |
| 2 | APROVADO | 200 | 128.384 | 1.558 | 5.655 | 6.971 | 7.61 | 0.97 |
| 3 | APROVADO | 200 | 119.792 | 1.67 | 5.346 | 6.445 | 7.3 | 0.99 |
| **média** | | 200 | 124.143 | 1.613 | 5.497 | 6.707 | 7.483 | 0.983 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=20, c10=20, c2=20, c3=20, c4=20, c5=20, c6=20, c7=20, c8=20, c9=20] · espera [c1=5.437, c10=5.432, c2=5.565, c3=5.457, c4=5.509, c5=5.36, c6=5.309, c7=5.53, c8=5.533, c9=5.699]
- semente 2: acessos [c1=20, c10=20, c2=20, c3=20, c4=20, c5=20, c6=20, c7=20, c8=20, c9=20] · espera [c1=5.617, c10=5.625, c2=5.643, c3=5.809, c4=5.732, c5=5.694, c6=5.749, c7=5.649, c8=5.67, c9=5.615]
- semente 3: acessos [c1=20, c10=20, c2=20, c3=20, c4=20, c5=20, c6=20, c7=20, c8=20, c9=20] · espera [c1=5.234, c10=5.448, c2=5.326, c3=5.434, c4=5.3, c5=5.497, c6=5.237, c7=5.563, c8=5.351, c9=5.38]

### contention | sync=etcd | semáforo K=2 | 5 nós × 5 clientes

- Carga: cada cliente: 10 pedidos, pausa 0.0–0.0 s (total 50 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s · etapa do semáforo: 0.2–1.0 s
- Repetições: 1 (semente 1)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 50 | 36.03 | 1.388 | 2.106 | 2.783 | 5.92 | 0.96 |

Etapa do semáforo (K=2):

| semente | pico de permissões | ocupação média | utilização | espera mediana etapa (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|
| 1 | 2 | 0.94 | 47% | 0.04 | 0.899 | 4.6 | 0.2 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=10, c2=10, c3=10, c4=10, c5=10] · espera [c1=2.156, c2=2.257, c3=1.842, c4=1.871, c5=1.905]

### contention | sync=etcd | semáforo K=2 | 5 nós × 5 clientes

- Carga: cada cliente: 20 pedidos, pausa 0.0–0.0 s (total 100 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s · etapa do semáforo: 0.2–1.0 s
- Repetições: 3 (sementes 1, 2, 3)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 100 | 63.133 | 1.584 | 1.868 | 2.504 | 5.98 | 0.99 |
| 2 | APROVADO | 100 | 63.373 | 1.578 | 1.829 | 2.63 | 5.98 | 0.99 |
| 3 | APROVADO | 100 | 62.051 | 1.612 | 1.84 | 2.653 | 5.98 | 0.99 |
| **média** | | 100 | 62.852 | 1.591 | 1.846 | 2.596 | 5.98 | 0.99 |

Etapa do semáforo (K=2):

| semente | pico de permissões | ocupação média | utilização | espera mediana etapa (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|
| 1 | 2 | 0.97 | 49% | 0.01 | 0.114 | 4.3 | 0.1 |
| 2 | 2 | 1 | 50% | 0.011 | 0.12 | 4.42 | 0.14 |
| 3 | 2 | 1.03 | 51% | 0.013 | 0.299 | 4.48 | 0.16 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=2.003, c2=1.829, c3=1.906, c4=1.829, c5=1.693]
- semente 2: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=1.791, c2=1.849, c3=1.739, c4=1.853, c5=1.907]
- semente 3: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=1.846, c2=1.79, c3=1.667, c4=1.853, c5=1.927]

### contention | sync=etcd | semáforo K=3 | 5 nós × 5 clientes

- Carga: cada cliente: 20 pedidos, pausa 0.0–0.0 s (total 100 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s · etapa do semáforo: 0.2–1.0 s
- Repetições: 3 (sementes 1, 2, 3)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 100 | 63.912 | 1.565 | 1.856 | 2.888 | 5.98 | 0.99 |
| 2 | APROVADO | 100 | 62.728 | 1.594 | 1.735 | 2.629 | 5.98 | 0.99 |
| 3 | APROVADO | 100 | 63.536 | 1.574 | 1.833 | 2.623 | 5.98 | 0.99 |
| **média** | | 100 | 63.392 | 1.578 | 1.808 | 2.713 | 5.98 | 0.99 |

Etapa do semáforo (K=3):

| semente | pico de permissões | ocupação média | utilização | espera mediana etapa (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|
| 1 | 3 | 0.97 | 32% | 0.011 | 0.282 | 4.12 | 0.03 |
| 2 | 3 | 1.02 | 34% | 0.012 | 0.041 | 4.12 | 0.03 |
| 3 | 3 | 0.99 | 33% | 0.017 | 0.067 | 4.12 | 0.03 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=1.935, c2=1.966, c3=1.868, c4=1.888, c5=1.749]
- semente 2: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=1.585, c2=1.783, c3=1.741, c4=1.685, c5=1.985]
- semente 3: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=1.826, c2=1.908, c3=1.705, c4=1.855, c5=1.849]

### contention | sync=etcd | semáforo K=3 | 5 nós × 5 clientes

- Carga: cada cliente: 10 pedidos, pausa 0.0–0.0 s (total 50 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s · etapa do semáforo: 1.0–2.0 s
- Repetições: 1 (semente 1)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 50 | 35.04 | 1.427 | 1.022 | 1.637 | 5.8 | 0.9 |

Etapa do semáforo (K=3):

| semente | pico de permissões | ocupação média | utilização | espera mediana etapa (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|
| 1 | 3 | 2.29 | 76% | 0.013 | 0.841 | 5.36 | 0.34 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=10, c2=10, c3=10, c4=10, c5=10] · espera [c1=1.034, c2=0.781, c3=1.136, c4=1.091, c5=0.832]

### contention | sync=none | 5 nós × 5 clientes

- Carga: cada cliente: 10 pedidos, pausa 0.0–0.0 s (total 50 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s
- Repetições: 1 (semente 1)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | detectou sobreposição | 50 | 7.723 | 6.474 | 0 | 0.001 | 0 | 0 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=10, c2=10, c3=10, c4=10, c5=10] · espera [c1=0.0, c2=0.0, c3=0.0, c4=0.0, c5=0.0]

### contention | sync=none | 5 nós × 5 clientes

- Carga: cada cliente: 20 pedidos, pausa 0.0–0.0 s (total 100 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s
- Repetições: 1 (semente 1)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | detectou sobreposição | 100 | 14.427 | 6.931 | 0 | 0 | 0 | 0 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=0.0, c2=0.0, c3=0.0, c4=0.0, c5=0.0]

### contention | sync=none | semáforo K=2 | 5 nós × 5 clientes

- Carga: cada cliente: 20 pedidos, pausa 0.0–0.0 s (total 100 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s · etapa do semáforo: 0.2–1.0 s
- Repetições: 1 (semente 1)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | detectou sobreposição | 100 | 27.433 | 3.645 | 0 | 0 | 0 | 0 |

Etapa do semáforo (K=2):

| semente | pico de permissões | ocupação média | utilização | espera mediana etapa (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|
| 1 | 5 | 2.28 | 114% | 0 | 0 | 0 | 0 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=20, c2=20, c3=20, c4=20, c5=20] · espera [c1=0.0, c2=0.0, c3=0.0, c4=0.0, c5=0.0]

### uneven | sync=etcd | 5 nós × 5 clientes

- Carga: c1: 30 pedidos, pausa 0.0–0.3 s; c2,c3,c4,c5: 10 pedidos, pausa 1.0–5.0 s (total 70 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s
- Repetições: 1 (semente 1)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 70 | 46.362 | 1.51 | 0.907 | 1.831 | 5.69 | 0.84 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=30, c2=10, c3=10, c4=10, c5=10] · espera [c1=0.506, c2=1.14, c3=0.766, c4=1.188, c5=0.932]

### uneven | sync=etcd | 5 nós × 5 clientes

- Carga: c1: 50 pedidos, pausa 0.0–0.3 s; c2,c3,c4,c5: 20 pedidos, pausa 1.0–5.0 s (total 130 pedidos por execução)
- Seção crítica de R: 0.2–1.0 s
- Repetições: 1 (semente 1)

| semente | verificação | concluídos | tempo total (s) | vazão (acessos/s) | espera mediana R (s) | p95 (s) | ops etcd/entrada | notif. watch/entrada |
|---|---|---|---|---|---|---|---|---|
| 1 | APROVADO | 130 | 88.826 | 1.464 | 0.744 | 1.928 | 5.72 | 0.86 |

Acessos por cliente e espera mediana por cliente (s):

- semente 1: acessos [c1=50, c2=20, c3=20, c4=20, c5=20] · espera [c1=0.705, c2=0.801, c3=0.699, c4=0.729, c5=0.756]
