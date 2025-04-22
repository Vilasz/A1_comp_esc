from typing import Sequence, Tuple, Dict
import random
import hashlib

from metrics import METRICS

random.seed(42)  # reprodutibilidade dos exemplos / métricas


# WORKER 



def process_chunk(
    chunk: Sequence[Tuple[str, int, str, float, str, str]],
    loops: int,
) -> Tuple[
    Dict[Tuple[str, str], Tuple[float, int]],
    Dict[str, float],
    Dict[str, int],
]:
    rng = random.Random()
    sha = hashlib.sha256

    prod_center: Dict[Tuple[str, str], Tuple[float, int]] = {}
    canal_tot: Dict[str, float] = {}
    estado_qtd: Dict[str, int] = {}

    for prod, qtd, centro, preco, canal, uf in chunk:
        preco = preco * rng.uniform(0.95, 1.05)  # ligeira variação
        # 1) produto+centro
        v, c = prod_center.get((prod, centro), (0.0, 0))
        prod_center[(prod, centro)] = (v + preco * qtd, c + qtd)
        # 2) canal
        canal_tot[canal] = canal_tot.get(canal, 0.0) + preco * qtd
        # 3) estado
        estado_qtd[uf] = estado_qtd.get(uf, 0) + qtd

        # payload CPU‑bound
        payload = f"{prod}{qtd}{centro}{preco}".encode()
        for _ in range(loops):
            payload = sha(payload).digest()

    METRICS.counter("records_processed").inc(len(chunk))
    return prod_center, canal_tot, estado_qtd


# MERGE TOOLS 



def merge_pair(
    dest: Dict[Tuple[str, str], Tuple[float, int]],
    src: Dict[Tuple[str, str], Tuple[float, int]],
):
    for k, (v, q) in src.items():
        dv, dq = dest.get(k, (0.0, 0))
        dest[k] = (dv + v, dq + q)


def merge_num(dest: Dict[str, float], src: Dict[str, float]):
    for k, v in src.items():
        dest[k] = dest.get(k, 0.0) + v


def merge_int(dest: Dict[str, int], src: Dict[str, int]):
    for k, v in src.items():
        dest[k] = dest.get(k, 0) + v