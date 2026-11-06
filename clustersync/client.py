"""Cliente: faz de 10 a 50 pedidos de escrita em R ao seu nó do Cluster Sync.

Conhece APENAS um nó (NODE_URL). Cada pedido tem request_id único (<client>-<n>).
Entre pedidos espera um tempo aleatório (THINK_MIN..THINK_MAX, padrão 1 a 5 s).
"""
import random
import time

from .config import env_float, env_int, env_str
from .http_util import post_json, wait_http
from .logutil import JsonlLogger


def main():
    client_id = env_str("CLIENT_ID", "c1")
    node_url = env_str("NODE_URL", "http://localhost:9000")
    seed = env_str("SEED", "1")
    start_at = env_float("START_AT", 0.0)

    requests_n = env_int("REQUESTS", 10)
    think_min, think_max = env_float("THINK_MIN", 1.0), env_float("THINK_MAX", 5.0)
    heavy = env_str("HEAVY_CLIENT", "") == client_id  # cenário de carga desigual
    if heavy:
        requests_n = env_int("HEAVY_REQUESTS", requests_n)
        think_min = env_float("HEAVY_THINK_MIN", think_min)
        think_max = env_float("HEAVY_THINK_MAX", think_max)

    log = JsonlLogger(client_id)
    rng = random.Random(f"{seed}:{client_id}")
    log.log("CLIENT_START", node_url=node_url, planned=requests_n, think_min=think_min,
            think_max=think_max, heavy=heavy, seed=seed)

    wait_http(f"{node_url}/health")
    delay = start_at - time.time()  # largada conjunta (START_AT é só um gatilho de partida)
    if delay > 0:
        time.sleep(delay)

    t_start = time.monotonic()
    for seq in range(1, requests_n + 1):
        request_id = f"{client_id}-{seq}"
        t0 = time.monotonic()
        log.log("SEND", request_id=request_id)
        resp = post_json(f"{node_url}/request", {"client_id": client_id, "request_id": request_id},
                         timeout=3600, retries=1)
        if resp.get("status") != "COMMITTED":
            raise RuntimeError(f"resposta inesperada: {resp}")
        log.log("RECEIVED_COMMITTED", request_id=request_id,
                latency_s=round(time.monotonic() - t0, 6), node_wait_s=resp.get("wait_s"))
        if seq < requests_n:
            time.sleep(rng.uniform(think_min, think_max))

    log.log("CLIENT_DONE", completed=requests_n, total_s=round(time.monotonic() - t_start, 6))
    log.close()


if __name__ == "__main__":
    main()
