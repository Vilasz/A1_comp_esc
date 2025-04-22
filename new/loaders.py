import csv
import json
from pathlib import Path
from typing import List, Tuple



# CONSTANTES / CONFIG 

DEFAULT_CSV = Path("mock_data_db.csv")
DEFAULT_JSON = Path("mock_data_pedidos_novos.json")


# EXTRAÇÃO 

def load_csv(path: Path) -> List[Tuple[str, int, str, float, str, str]]:
    with path.open("r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        return [
            (
                row["produto"],
                int(row["quantidade"]),
                row["centro_logistico_mais_proximo"],
                float(row["preco_unitario"]),
                row["canal_venda"],
                row["estado_cliente"],
            )
            for row in rdr
        ]

def load_json(path: Path) -> List[Tuple[str, int, str, float, str, str]]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)["pedidos"]
    return [
        (
            p["produto"],
            int(p["quantidade"]),
            p["centro_logistico_mais_proximo"],
            float(p["preco_unitario"]),
            p["canal_venda"],
            p["estado_cliente"],
        )
        for p in data
    ]