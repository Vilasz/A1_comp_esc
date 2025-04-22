"""
Ferramentas de benchmark para o ETL (mvp_pipeline) *sem uso de pandas*.

Requisitos extra: plotly, tqdm
(opcional: kaleido para exportar PNG/SVG)
Dependência interna: dataframe.py (Series, DataFrame)
"""

from __future__ import annotations

import contextlib
import io
import time
from pathlib import Path
from typing import Sequence, Dict

import plotly.express as px
from tqdm import tqdm
from new.source.utils.dataframe import Series, DataFrame 

from new.source.framework.mvp_pipeline import run_pipeline

def benchmark_scaling(
    *,
    workers_seq: Sequence[int] | str,
    csv_size: int = 1_000_000,
    json_size: int = 400_000,
    loops: int = 800,
    regenerate: bool = False,
    chunksize: int | None = None,
    db_path: str | Path = "ecommerce.db",
    csv_path: str | Path = "bench_mock.csv",
    json_path: str | Path = "bench_mock.json",
    show_fig: bool = True,
    save_png: bool = True,
):
    """Roda o ETL em série para cada valor de *workers_seq* e devolve *(fig, df)*.

    Parameters
    ----------
    workers_seq : Sequence[int] | str
        Lista como ``[1,2,4,8]`` ou string ``"1,2,4,8"``.
    csv_size, json_size, loops : int
        Parâmetros passados ao gerador de dados e ao worker.
    regenerate : bool
        Se ``True``, força recriação dos datasets em cada execução (mais lento).
    chunksize : int | None
        Tamanho de chunk manual; se ``None``, segue heurística padrão.
    db_path, csv_path, json_path : str | Path
        Arquivos alvo; por padrão tudo na pasta corrente.
    show_fig : bool
        Exibe o gráfico interativo ao final.
    save_png : bool
        Salva *benchmark_scaling.png* se o back‑end ``kaleido`` estiver
        disponível (falha silenciosa caso contrário).

    Returns
    -------
    fig : plotly.graph_objs.Figure
    df  : dataframe.DataFrame (colunas: workers, wall_time)
    """

    if isinstance(workers_seq, str):
        workers_seq = [int(x) for x in workers_seq.split(",") if x.strip()]
    if not workers_seq:
        raise ValueError("workers_seq deve conter pelo menos um inteiro")

    workers_seq = sorted(set(workers_seq))
    print("Benchmark para workers =", workers_seq)

    csv_path = Path(csv_path)
    json_path = Path(json_path)
    db_path = Path(db_path)

    if regenerate or not csv_path.exists():
        from new.source.etl.data_generators import generate_csv

        generate_csv(csv_size, csv_path)
    if regenerate or not json_path.exists():
        from new.source.etl.data_generators import generate_json

        generate_json(json_size, json_path)

    results: Dict[int, float] = {}

    for w in tqdm(workers_seq, desc="Benchmark", unit="config"):
        t0 = time.perf_counter()
        run_pipeline(
            csv_path=csv_path,
            json_path=json_path,
            db_path=db_path,
            csv_size=csv_size,
            json_size=json_size,
            workers=w,
            loops=loops,
            chunksize=chunksize,
            regenerate=False,  # dados já gerados acima
        )
        results[w] = time.perf_counter() - t0

    workers_sorted = sorted(results.keys())
    wall_times_sorted = [results[w] for w in workers_sorted]

    df = DataFrame(
        columns=["workers", "wall_time"],
        series=[Series(workers_sorted), Series(wall_times_sorted)],
    )

    fig = px.line(
        x=workers_sorted,
        y=wall_times_sorted,
        markers=True,
        title="Escalonamento do ETL – Wall‑time × #Workers",
        labels={"x": "Workers", "y": "Tempo wall (s)"},
    )

    fig.update_traces(text=[f"{t:.1f}" for t in wall_times_sorted], textposition="top center")

    if show_fig:
        fig.show()

    if save_png:
        _save_last_plot(fig, "benchmark_scaling.png")

    return fig, df

def _save_last_plot(fig, fname: str, fmt: str = "png", scale: int = 2) -> None:
    """Tenta gravar *fname* no formato indicado usando o back‑end *kaleido*."""
    with contextlib.suppress(Exception):
        buf = io.BytesIO()
        fig.write_image(buf, format=fmt, scale=scale)
        Path(fname).write_bytes(buf.getvalue())
        print(f"📊  Gráfico salvo em {fname}")


if __name__ == "__main__":
    import sys

    workers_cli = sys.argv[1] if len(sys.argv) > 1 else "8,12"
    benchmark_scaling(workers_seq=workers_cli, csv_size=500_000, json_size=200_000)
