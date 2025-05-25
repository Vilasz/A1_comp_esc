import csv
import json
from pathlib import Path
from typing import List, Tuple



# CONSTANTES / CONFIG 

DEFAULT_CSV = Path("mock_data_db.csv")
DEFAULT_JSON = Path("mock_data_pedidos_novos.json")


# EXTRAÇÃO 


def load_csv(path: Path) -> List[Tuple[str, int, str, float, str, str]]:
    records = []
    with path.open("r", encoding="utf-8") as f:
        rdr = csv.DictReader(f) # DictReader is good
        for row in rdr:
            try:
                # Explicitly map from the potentially richer CSV row to the expected 6-tuple
                record = (
                    row["produto"], # Assumes 'produto' column exists
                    int(row["quantidade"]), # Assumes 'quantidade' column exists
                    row["centro_logistico_mais_proximo"], # ...and so on
                    float(row["preco_unitario"]), # Make sure it's 'preco_unitario' not just 'preco'
                    row["canal_venda"],
                    row["estado_cliente"],
                )
                records.append(record)
            except KeyError as e:
                print(f"Skipping row due to missing key {e} in CSV: {row}")
                continue
            except ValueError as e:
                print(f"Skipping row due to value error {e} in CSV: {row}")
                continue
    return records

def load_json(path: Path) -> List[Tuple[str, int, str, float, str, str]]:
    records = []
    with path.open("r", encoding="utf-8") as f:
        full_json_data = json.load(f) # Load the entire JSON object

    # Check if "pedidos" key exists and is a list
    if "pedidos" not in full_json_data or not isinstance(full_json_data["pedidos"], list):
        print(f"Warning: JSON file {path} does not contain a 'pedidos' list as expected.")
        return [] # Return empty list if the structure is not as expected

    list_of_order_dicts = full_json_data["pedidos"] # Get the actual list of orders

    for order_dict in list_of_order_dicts: # Iterate over the dictionaries in the list
        try:
            # Ensure order_dict is actually a dictionary before trying to access keys
            if not isinstance(order_dict, dict):
                print(f"Skipping non-dictionary item in 'pedidos' list: {order_dict}")
                continue

            record = (
                order_dict["produto"],
                int(order_dict["quantidade"]),
                order_dict["centro_logistico_mais_proximo"],
                float(order_dict["preco_unitario"]),
                order_dict["canal_venda"],
                order_dict["estado_cliente"],
            )
            records.append(record)
        except KeyError as e:
            print(f"Skipping order due to missing key {e} in JSON record: {order_dict}")
            continue
        except ValueError as e: # For int() or float() conversion errors
            print(f"Skipping order due to value error '{e}' in JSON record: {order_dict}")
            continue
        except TypeError as e: # Catch other potential type errors if order_dict isn't a dict
            print(f"Skipping order due to type error '{e}' (item not a dict?) in JSON record: {order_dict}")
            continue
    return records