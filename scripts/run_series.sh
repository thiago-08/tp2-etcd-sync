#!/usr/bin/env bash
# 3 repetições (sementes 1, 2, 3) de um cenário. Exige as métricas do enunciado.
# uso: scripts/run_series.sh <normal|contention|uneven> [opções do run_experiment.py]
# ex.: scripts/run_series.sh contention --requests 20
set -u
scenario=${1:?uso: scripts/run_series.sh <normal|contention|uneven> [opções...]}
shift
cd "$(dirname "$0")/.."
for seed in 1 2 3; do
  echo "=============== $scenario | repetição com semente $seed ==============="
  python scripts/run_experiment.py --scenario "$scenario" --seed "$seed" "$@" || echo "(repetição $seed reprovou — veja o log)"
done
