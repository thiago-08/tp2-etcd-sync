"""Helpers HTTP (stdlib) usados entre clientes, nós e observador."""
import json
import time
import urllib.error
import urllib.request


def post_json(url: str, payload: dict, timeout: float = 30.0, retries: int = 5) -> dict:
    """POST JSON. Só repete em falha de conexão; erro HTTP é propagado direto."""
    data = json.dumps(payload).encode()
    last = None
    for attempt in range(retries):
        req = urllib.request.Request(
            url, data=data, headers={"Content-Type": "application/json"}, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode() or "{}")
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"POST {url} -> HTTP {exc.code}: {exc.read().decode()[:200]}")
        except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
            last = exc
            time.sleep(0.2 * (attempt + 1))
    raise RuntimeError(f"POST {url} falhou: {last}")


def get_json(url: str, timeout: float = 5.0) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode() or "{}")


def wait_http(url: str, timeout: float = 60.0) -> None:
    """Espera um endpoint /health responder (nós e observador sobem em paralelo)."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            get_json(url, timeout=2.0)
            return
        except Exception:
            if time.monotonic() > deadline:
                raise RuntimeError(f"{url} não respondeu em {timeout}s")
            time.sleep(0.3)
