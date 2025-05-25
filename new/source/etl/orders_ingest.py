# orders_ingest_refactored.py
from __future__ import annotations

import datetime as dt
import random
import sqlite3 # Direct SQLite usage
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

# --- Project-specific imports ---
# Assuming dataframe.py is at new.source.utils.dataframe
from new.source.utils.dataframe import DataFrame, read_json # Using the DataFrame's read_json
# If ETL_utils.RepoData is preferred for consistency, that's an option too.
# from new.source.etl.ETL_utils import RepoData

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)8s] %(message)s")


DB_FILE = Path("ecommerce.db") # Or from a central config
MOCK_JSON = Path("../mock/mock_data_pedidos_novos.json") # Or from a central config

# --- Helper Database Functions (to replace PedidoService functionalities) ---
# These would ideally live in a shared database utility module if used widely.

def get_last_pk(conn: sqlite3.Connection, table_name: str, pk_column: str = "id") -> int:
    cursor = conn.cursor()
    # Ensure table_name and pk_column are not from unsanitized input if this were more general
    cursor.execute(f"SELECT MAX({pk_column}) FROM {table_name}")
    result = cursor.fetchone()
    return result[0] if result and result[0] is not None else 0

def get_row_by_pk(conn: sqlite3.Connection, table_name: str, pk_value: Any, pk_column: str = "id") -> Optional[Dict[str, Any]]:
    cursor = conn.cursor()
    # Using placeholders for values is crucial for security
    cursor.execute(f"SELECT * FROM {table_name} WHERE {pk_column} = ?", (pk_value,))
    row_tuple = cursor.fetchone()
    if row_tuple:
        return dict(zip([desc[0] for desc in cursor.description], row_tuple))
    return None

def fetch_all_rows_as_dicts(conn: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

# --- 1. INGESTÃO de pedidos + itens_pedido + atualização de estoque ---

def process_new_orders_refactored() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    • Lê mock_data_pedidos_novos.json
    • Inserts rows into 'pedidos' and 'itens_pedido' tables.
    • Updates stock in the 'produtos' table.
    • Returns the newly inserted rows as lists of dictionaries.
    """
    logger.info(f"Processing new orders from {MOCK_JSON}")

    # --- Extract JSON Data ---
    # Option 1: Using read_json from your DataFrame utility (if it returns a simple list of dicts or records)
    # This assumes read_json can return a format like List[Tuple] or List[Dict]
    # For this example, I'll assume it returns a list of lists/tuples matching orders_raw format.
    # orders_raw_df = read_json(MOCK_JSON) # If it returns a DataFrame
    # orders_raw = [ (row['cli_id'], row['prod_id'], ...) for row in orders_raw_df.to_records() ] # Example conversion

    # Let's stick to the original structure of orders_raw if JSONExtractor().extract() produced it
    # If JSONExtractor from ETL.py (ETL_new.py) is just a wrapper around read_json:
    try:
        # Simplified extraction for this script's purpose.
        # If this were an ETL_new.py task, it would go through the full pipeline.
        # For this direct script, we can use read_json or a direct json.load
        import json
        with open(MOCK_JSON, 'r') as f:
            # Assuming the JSON structure is a list of [cli_id, prod_id, qtd, centro_id] lists/tuples
            # or a dict that needs parsing. The original JSONExtractor().extract() hid this.
            # Let's assume the raw data structure from the original script was:
            # List[Tuple[cli_id, prod_id, qtd, centro_id]]
            data = json.load(f)
            # This part is highly dependent on MOCK_JSON's actual structure
            # and what JSONExtractor().extract() used to do.
            # If MOCK_JSON is like {"pedidos": [[cli, prod, qtd, centro], ...]}
            if "pedidos" in data and isinstance(data["pedidos"], list):
                raw_order_dicts = data["pedidos"]
            else:
                logger.error(f"Expected 'pedidos' key with a list in {MOCK_JSON}")
                return [], []

            orders_raw: List[Tuple[int, int, int, str]] = []


            for order_dict in raw_order_dicts:
                try:
                    # Extract the specific fields your original orders_raw tuple expected for processing.
                    # The original orders_raw was: (cli_id, prod_id, qtd, centro_id)
                    # data_generators makes "centro_logistico_mais_proximo" a string, e.g. "São Paulo"
                    # Your DB schema for pedidos.centro_logistico_id expects an INTEGER.
                    # This implies a lookup or mapping is needed if you use the string name directly.
                    # For now, I'll assume you need the *name* of the center as previously.
                    # If you need the ID, you'll need a mapping from center name to ID.
                    cli_id = int(order_dict["cliente_id"])
                    prod_id = int(order_dict["produto_id"])
                    qtd = int(order_dict["quantidade"])
                    centro_name = str(order_dict["centro_logistico_mais_proximo"]) # This is a NAME
                    # If your `process_new_orders_refactored` loop expects centro_id as an integer ID,
                    # you need to map centro_name to an ID here (e.g. query centros_logisticos table).
                    # For now, assuming the loop was adapted or the original `orders_raw` used names.
                    orders_raw.append((cli_id, prod_id, qtd, centro_name))
                except (KeyError, ValueError) as e:
                    logger.warning(f"Skipping order due to missing/invalid data in {MOCK_JSON} item: {order_dict}. Error: {e}")
                    continue
            logger.info(f"Extracted {len(orders_raw)} raw order entries.")

    except FileNotFoundError:
        logger.error(f"Mock JSON file not found: {MOCK_JSON}")
        return [], []
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {MOCK_JSON}")
        return [], []
    except Exception as e:
        logger.error(f"Error reading or processing JSON file {MOCK_JSON}: {e}", exc_info=True)
        return [], []


    inserted_pedidos_rows = []
    inserted_itens_pedido_rows = []

    conn = sqlite3.connect(DB_FILE)
    try:
        with conn: # Context manager handles begin transaction and commit/rollback
            cursor = conn.cursor()

            # Get initial PKs
            next_pedido_id = get_last_pk(conn, "pedidos") + 1
            next_item_id = get_last_pk(conn, "itens_pedido") + 1
            now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for cli_id, prod_id, qtd, centro_id in orders_raw:
                # Fetch related data
                cliente_info = get_row_by_pk(conn, "clientes", cli_id)
                produto_info = get_row_by_pk(conn, "produtos", prod_id)

                if not cliente_info:
                    logger.warning(f"Cliente ID {cli_id} not found. Skipping order.")
                    continue
                if not produto_info:
                    logger.warning(f"Produto ID {prod_id} not found. Skipping order item.")
                    continue

                # --- Insert Pedido ---
                valor_total = round(random.uniform(50, 5_000), 2) # Original logic
                endereco_json = cliente_info["endereco_json"]     # Original logic

                pedido_data = {
                    "id": next_pedido_id, "cliente_id": cli_id, "data_pedido": now_str,
                    "status": "novo", "centro_logistico_id": centro_id,
                    "valor_total": valor_total, "endereco_entrega_json": endereco_json,
                }
                cursor.execute(
                    """INSERT INTO pedidos (id, cliente_id, data_pedido, status, centro_logistico_id, valor_total, endereco_entrega_json)
                       VALUES (:id, :cliente_id, :data_pedido, :status, :centro_logistico_id, :valor_total, :endereco_entrega_json)""",
                    pedido_data
                )
                inserted_pedidos_rows.append(pedido_data)

                # --- Insert Item Pedido ---
                preco_unitario = produto_info["preco"] # Original logic
                item_pedido_data = {
                    "id": next_item_id, "pedido_id": next_pedido_id, "produto_id": prod_id,
                    "quantidade": qtd, "preco_unitario": preco_unitario,
                }
                cursor.execute(
                    """INSERT INTO itens_pedido (id, pedido_id, produto_id, quantidade, preco_unitario)
                       VALUES (:id, :pedido_id, :produto_id, :quantidade, :preco_unitario)""",
                    item_pedido_data
                )
                inserted_itens_pedido_rows.append(item_pedido_data)

                # --- Update Estoque ---
                # Ensure 'estoque' column exists and is numeric. The original used "estoque - ?"
                # which is a SQLite-specific way to decrement.
                cursor.execute(
                    "UPDATE produtos SET estoque = estoque - ? WHERE id = ?",
                    (qtd, prod_id)
                )
                # For safety, you might want to check if (estoque - qtd) < 0 and handle it.

                next_pedido_id += 1
                next_item_id += 1
            
            # conn.commit() is called automatically by `with conn:` on successful exit
            logger.info(f"Successfully processed and inserted {len(inserted_pedidos_rows)} orders and {len(inserted_itens_pedido_rows)} items.")

    except sqlite3.Error as e:
        logger.error(f"Database error during order processing: {e}", exc_info=True)
        # conn.rollback() is called automatically by `with conn:` on exception
        return [], [] # Return empty on error
    finally:
        if conn:
            conn.close()
            
    return inserted_pedidos_rows, inserted_itens_pedido_rows


# --- 2. CÁLCULOS auxiliares (adapted to work with lists of dicts) ---
# The analytical functions now need to fetch their data directly or operate on the passed-in lists.
# For simplicity and to match the previous pattern of operating on "Table-like" objects,
# these functions will now take List[Dict[str, Any]] which represents rows from a table.

def top5_mais_vendidos(itens_pedido_rows: List[Dict[str, Any]]) -> Dict[int, int]:
    vendas: Dict[int, int] = {}
    for row in itens_pedido_rows:
        vendas[row["produto_id"]] = vendas.get(row["produto_id"], 0) + row["quantidade"]
    # Sort by quantity (value), take top 5, then convert to dict.
    return dict(sorted(vendas.items(), key=lambda kv: kv[1], reverse=True)[:5])


def valor_total_vendas(pedidos_rows: List[Dict[str, Any]]) -> float:
    return sum(r["valor_total"] for r in pedidos_rows)


def vendas_por_cat_produto(
    produtos_rows: List[Dict[str, Any]],
    itens_pedido_rows: List[Dict[str, Any]]
) -> Dict[int, int]:
    result: Dict[int, int] = {}
    # Create a quick lookup for product_id to categoria_id
    produto_to_cat_map = {p["id"]: p["categoria_id"] for p in produtos_rows}

    for row in itens_pedido_rows:
        cat = produto_to_cat_map.get(row["produto_id"])
        if cat is not None:
            result[cat] = result.get(cat, 0) + row["quantidade"]
    return result


def centros_mais_requisitados(pedidos_rows: List[Dict[str, Any]]) -> Dict[int, int]:
    freq: Dict[int, int] = {}
    for r in pedidos_rows:
        freq[r["centro_logistico_id"]] = freq.get(r["centro_logistico_id"], 0) + 1
    # Sort by frequency (value), then convert to dict.
    return dict(sorted(freq.items(), key=lambda kv: kv[1], reverse=True))


def alerta_reposicao(produtos_rows: List[Dict[str, Any]]) -> List[str]:
    return [r["nome"] for r in produtos_rows if r.get("estoque", float('inf')) <= 10]


if __name__ == "__main__":
    logger.info("Starting order ingestion and analysis script.")
    
    # Process new orders and get the data for analytics
    # These are the *newly inserted* rows, not the whole table.
    newly_inserted_pedidos, newly_inserted_itens = process_new_orders_refactored()

    if not newly_inserted_pedidos:
        logger.info("No new orders were processed. Exiting analytics part.")
    else:
        print("\n--- Analytics on NEWLY PROCESSED Orders ---")
        print(f"Top-5 produtos MAIS VENDIDOS (from this batch): {top5_mais_vendidos(newly_inserted_itens)}")
        print(f"Valor total das vendas (from this batch): R$ {valor_total_vendas(newly_inserted_pedidos):,.2f}")
        print(f"Centros logísticos MAIS USADOS (from this batch): {centros_mais_requisitados(newly_inserted_pedidos)}")

        # For analytics requiring full table data, we need to fetch it:
        conn_main = sqlite3.connect(DB_FILE)
        try:
            logger.info("\n--- Analytics on FULL Database Tables ---")
            all_produtos_rows = fetch_all_rows_as_dicts(conn_main, "produtos")
            all_itens_pedido_rows = fetch_all_rows_as_dicts(conn_main, "itens_pedido") # If needed for full scope vendas_por_cat
            
            print(f"Vendas por categoria de produto (full scope): {vendas_por_cat_produto(all_produtos_rows, all_itens_pedido_rows)}")
            print(f"Alertas de estoque (full scope): {alerta_reposicao(all_produtos_rows)}")
        finally:
            if conn_main:
                conn_main.close()
    
    logger.info("Script finished.")