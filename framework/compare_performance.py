import concurrent.futures
from multiprocessing import JoinableQueue, cpu_count, set_start_method
import threading
import traceback
import os
import time
import queue
import random
import sys
from typing import List, Any, Optional, Callable

# Ajustamos o start method do multiprocessing para 'spawn' (ou 'forkserver')
try:
    set_start_method('spawn')
except RuntimeError:
    pass


# =============================================================================
#                 Funções de simulação de trabalho pesado
# =============================================================================
def _simulate_heavy_computation(x: int) -> int:
    """
    Simula uma operação matemática intensiva em CPU (ex.: soma de divisores).
    """
    total = 0
    limit = int(x**0.5) + 1
    for i in range(1, limit):
        if x % i == 0:
            total += i
            d2 = x // i
            if d2 != i:
                total += d2
    return total

def filter_worker(chunk: List[int]) -> Optional[List[int]]:
    """Etapa 1: Filtra valores com 'soma de divisores' acima de certo threshold."""
    threshold = 10000
    output = []
    for val in chunk:
        sdiv = _simulate_heavy_computation(val)
        if sdiv > threshold:
            output.append(val)
    return output if output else None

def transform_worker(chunk: List[int]) -> Optional[List[int]]:
    """Etapa 2: Aplica outro cálculo pesado em cada item."""
    if not chunk:
        return None
    result = []
    for val in chunk:
        tmp1 = _simulate_heavy_computation(val)
        tmp2 = _simulate_heavy_computation(tmp1)
        result.append(tmp2)
    return result

def sort_worker(chunk: List[int]) -> Optional[List[int]]:
    """Etapa 3: Ordena o chunk e faz uma soma parcial."""
    if not chunk:
        return None
    sorted_data = sorted(chunk)
    _ = sum(sorted_data[:3000])  # mini-soma para simular cálculo
    return sorted_data

def print_worker(chunk: List[int]) -> None:
    """Etapa 4: Apenas imprime as últimas posições."""
    if not chunk:
        print("[Print] Chunk vazio.")
        return
    print("[Print] top 5 =>", chunk[-5:])


# =============================================================================
#                  PIPELINE (mesmo conceito de antes)
# =============================================================================
class ConcurrentPipeline:
    SENTINEL = None

    def __init__(self,
                 num_workers: Optional[int] = None,
                 max_buffer_size: int = 10,
                 verbose: bool = False):
        self.num_workers = num_workers or cpu_count()
        self.max_buffer_size = max_buffer_size
        self.verbose = verbose

        self.stages = []
        self.queues = []
        self.pool = None
        self.dispatchers = []
        self.is_running = False
        self.start_time = None
        self.processed_items = 0

    def add_stage(self, name: str, worker_fn: Callable[[Any], Any]) -> None:
        stage = {'name': name, 'worker': worker_fn}
        self.stages.append(stage)

    def _setup_pipeline(self):
        if not self.stages:
            raise ValueError("No stages.")
        self.queues = []
        for _ in range(len(self.stages) + 1):
            self.queues.append(JoinableQueue(maxsize=self.max_buffer_size))

        # Executor
        self.pool = concurrent.futures.ProcessPoolExecutor(max_workers=self.num_workers)

        # Cria threads
        self.dispatchers = []
        for i, st in enumerate(self.stages):
            inp = self.queues[i]
            outp = self.queues[i+1] if i < len(self.stages)-1 else None
            dname = f"Dispatcher-{st['name']}"

            t = threading.Thread(target=self._dispatcher_runner,
                                 args=(dname, inp, outp, st['worker']),
                                 daemon=True)
            self.dispatchers.append(t)

    def _dispatcher_runner(self,
                           dispatcher_name: str,
                           input_q: JoinableQueue,
                           output_q: Optional[JoinableQueue],
                           worker_fn: Callable[[Any], Any]):
        sentinel_received = False
        while True:
            try:
                data = input_q.get(timeout=1.0)
                if data is self.SENTINEL:
                    sentinel_received = True
                    input_q.task_done()
                else:
                    # Submete ao pool
                    f = self.pool.submit(worker_fn, data)
                    f.add_done_callback(
                        lambda fut: self._callback_result(fut, output_q, input_q)
                    )
            except queue.Empty:
                if sentinel_received and input_q.empty():
                    break
                continue
            except Exception as e:
                traceback.print_exc()
                time.sleep(0.2)

        if output_q:
            output_q.put(self.SENTINEL)

    def _callback_result(self,
                         future: concurrent.futures.Future,
                         output_q: Optional[JoinableQueue],
                         input_q: JoinableQueue):
        try:
            if future.exception():
                traceback.print_exc()
            else:
                res = future.result()
                if output_q and res is not None:
                    output_q.put(res)
                    self.processed_items += 1
        except:
            traceback.print_exc()
        finally:
            input_q.task_done()

    def start(self):
        if self.is_running:
            raise RuntimeError("Already running.")
        self._setup_pipeline()
        for d in self.dispatchers:
            d.start()
        self.is_running = True
        self.start_time = time.time()

    def feed_data(self, data: Any):
        if not self.is_running:
            raise RuntimeError("Not running.")
        self.queues[0].put(data)

    def end(self):
        if not self.is_running:
            return
        self.queues[0].put(self.SENTINEL)
        self.wait_completion()
        self.shutdown()

    def wait_completion(self):
        for q in self.queues:
            q.join()

    def shutdown(self):
        if self.pool:
            self.pool.shutdown(wait=False)
        for d in self.dispatchers:
            d.join(2)
        self.is_running = False
        elapsed = time.time() - (self.start_time or time.time())
        # se quiser, printa algo
        # print(f"[Pipeline] Processed {self.processed_items} in {elapsed:.2f}s")


# =============================================================================
#                 FUNÇÃO SEQUENCIAL (PARA BASELINE)
# =============================================================================
def run_sequential(chunks: List[List[int]]) -> None:
    """
    Roda as mesmas etapas (Filter -> Transform -> Sort -> Print),
    mas sequencialmente, sem multiprocessing.
    """
    for chunk in chunks:
        # Filtro
        filt_result = filter_worker(chunk)
        if not filt_result:
            continue
        # Transform
        transf_result = transform_worker(filt_result)
        if not transf_result:
            continue
        # Sort
        sorted_result = sort_worker(transf_result)
        if sorted_result is not None:
            # Print
            print_worker(sorted_result)


# =============================================================================
#                    FUNÇÃO PARA TESTAR O PIPELINE
# =============================================================================
def run_pipeline(num_workers: int, chunks: List[List[int]]) -> None:
    """
    Roda o pipeline com um certo número de workers em ProcessPoolExecutor.
    """
    pipeline = ConcurrentPipeline(num_workers=num_workers, max_buffer_size=4, verbose=False)
    pipeline.add_stage("Filter", filter_worker)
    pipeline.add_stage("Transform", transform_worker)
    pipeline.add_stage("Sort", sort_worker)
    pipeline.add_stage("Print", print_worker)

    pipeline.start()
    for c in chunks:
        pipeline.feed_data(c)
    pipeline.end()


# =============================================================================
#                          MAIN DE COMPARAÇÃO
# =============================================================================
def main():
    # Parâmetros de teste
    MIN_VAL = 1
    MAX_VAL = 300_000
    CHUNK_SIZE = 40_000
    NUM_CHUNKS = 5

    # Gera os chunks de dados
    print(f"\nGerando {NUM_CHUNKS} chunks de {CHUNK_SIZE} itens cada, no intervalo [{MIN_VAL}, {MAX_VAL}]...")
    chunks = []
    for _ in range(NUM_CHUNKS):
        chunk_data = [random.randint(MIN_VAL, MAX_VAL) for _ in range(CHUNK_SIZE)]
        chunks.append(chunk_data)
    print("Ok, dados gerados.\n")

    # 1) Teste SEQUENCIAL
    print("=== TESTE SEQUENCIAL (Baseline) ===")
    t0_seq = time.time()
    run_sequential(chunks)
    t1_seq = time.time()
    seq_time = t1_seq - t0_seq
    print(f"Tempo total SEQUENCIAL: {seq_time:.2f} s\n")

    # 2) Teste com diferentes números de workers
    cpu_total = cpu_count()
    worker_values = [1, 2, 4, 6, 8, 10, cpu_total, max(1, cpu_total - 1)]
    # Se quiser, pode ordenar e remover duplicados
    worker_values = list(set(worker_values))
    worker_values.sort()

    print("=== TESTE PIPELINE CONCORRENTE ===")
    for w in worker_values:
        print(f"\n--- Rodando com num_workers={w} ---")
        # Precisamos regenerar os dados ou apenas reaproveitar?
        # Para ser consistente, use a MESMA base se for comparar o mesmo processamento.
        # Reaproveitamos 'chunks'.
        # Se quiser total equivalência, re-clone a lista para não perder dados por causa do pipeline.
        # Mas aqui, cada chunk não é alterado pela pipeline, então ok.

        t0_par = time.time()
        run_pipeline(w, chunks)
        t1_par = time.time()
        elapsed_par = t1_par - t0_par
        print(f"Tempo total com {w} workers = {elapsed_par:.2f} s")

    print("\n=== COMPARAÇÃO FINAL ===")
    print(f"  -> Tempo SEQUENCIAL : {seq_time:.2f} s")
    # Você vê os tempos do pipeline acima para cada w.
    print("\nFim da comparação.")


if __name__ == "__main__":
    main()
