# --- orders_processing_logic.py (ou similar) ---
import sqlite3
import datetime as dt
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

from new.source.utils.dataframe import DataFrame

logger = logging.getLogger(__name__)

# --- Funções Auxiliares de Banco de Dados ---
# (Estas são essenciais e precisam estar acessíveis aos workers/stages)

def get_db_connection(db_path: Path) -> sqlite3.Connection:
    """Cria e retorna uma conexão SQLite."""
    try:
        conn = sqlite3.connect(db_path, timeout=10) # Timeout pode ser útil
        conn.row_factory = sqlite3.Row # Facilita o acesso por nome de coluna
        return conn
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados {db_path}: {e}")
        raise

def get_row_by_pk(conn: sqlite3.Connection, table_name: str, pk_value: Any, pk_column: str = "id") -> Optional[sqlite3.Row]:
    """Busca uma linha pela chave primária."""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE {pk_column} = ?", (pk_value,))
        return cursor.fetchone()
    except Exception as e:
        logger.error(f"Erro ao buscar PK {pk_value} em {table_name}: {e}")
        return None

def get_last_pk(conn: sqlite3.Connection, table_name: str, pk_column: str = "id") -> int:
    """Pega o último ID para saber o próximo (ATENÇÃO: Risco em alta concorrência)."""
    # NOTA: Em um ambiente de alta concorrência real, usar AUTOINCREMENT do DB
    # ou um gerador de IDs centralizado seria mais seguro.
    # Para este pipeline, vamos assumir que podemos gerenciar IDs sequencialmente
    # se a carga de pedidos for feita de forma controlada.
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX({pk_column}) FROM {table_name}")
        result = cursor.fetchone()
        return result[0] if result and result[0] is not None else 0
    except Exception as e:
        logger.error(f"Erro ao buscar MAX PK em {table_name}: {e}")
        return 0 # Ou relançar o erro


# --- Função de Transformação Principal ---

def transform_new_orders_worker(
    raw_orders_df: DataFrame,
    db_path: Path,
    start_pedido_id: int, # Passar o ID inicial para este lote
    start_item_id: int # Passar o ID inicial para este lote
) -> Tuple[Optional[DataFrame], Optional[DataFrame]]:
    """
    Worker para transformar pedidos brutos.
    Busca dados de clientes/produtos e prepara DFs para 'pedidos' e 'itens_pedido'.
    Retorna (df_pedidos, df_itens_pedido).
    """
    conn = None
    try:
        conn = get_db_connection(db_path)
        pedidos_data = []
        itens_data = []
        now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_pedido_id = start_pedido_id
        current_item_id = start_item_id

        logger.info(f"Worker: Transformando {raw_orders_df.shape[0]} pedidos. IDs Pedido: {start_pedido_id}..., Item: {start_item_id}...")

        # Mapeamento de Centro Logístico (Exemplo - Carregar do DB seria melhor)
        # Assumindo que o JSON vem com NOME, mas o DB quer ID (Ex: 1='São Paulo')
        centro_map = {"São Paulo": 1, "Rio de Janeiro": 2, "Belo Horizonte": 3} # Exemplo!

        for row in raw_orders_df.to_records():
            cli_id = row.get("cliente_id")
            prod_id = row.get("produto_id")
            qtd = row.get("quantidade")
            centro_name = row.get("centro_logistico_mais_proximo")

            if not all([cli_id, prod_id, qtd, centro_name]):
                logger.warning(f"Worker: Pulando pedido por dados ausentes: {row}")
                continue

            cliente_info = get_row_by_pk(conn, "clientes", cli_id)
            produto_info = get_row_by_pk(conn, "produtos", prod_id)

            if not cliente_info:
                logger.warning(f"Worker: Cliente ID {cli_id} não encontrado. Pulando pedido.")
                continue
            if not produto_info:
                logger.warning(f"Worker: Produto ID {prod_id} não encontrado. Pulando item.")
                continue

            # Mapeia nome para ID, usa 0 ou None se não encontrar
            centro_id = centro_map.get(centro_name, 0) # Use 0 ou trate erro

            # --- Prepara Pedido ---
            pedido_dict = {
                "id": current_pedido_id,
                "cliente_id": cli_id,
                "data_pedido": now_str,
                "status": "processado_etl", # Novo status
                "centro_logistico_id": centro_id,
                "valor_total": float(produto_info["preco"]) * int(qtd), # Cálculo real
                "endereco_entrega_json": cliente_info["endereco_json"],
            }
            pedidos_data.append(pedido_dict)

            # --- Prepara Item Pedido ---
            item_dict = {
                "id": current_item_id,
                "pedido_id": current_pedido_id,
                "produto_id": prod_id,
                "quantidade": qtd,
                "preco_unitario": float(produto_info["preco"]),
            }
            itens_data.append(item_dict)

            current_pedido_id += 1
            current_item_id += 1

        if not pedidos_data:
            return None, None

        df_pedidos = DataFrame(data=pedidos_data)
        df_itens = DataFrame(data=itens_data)
        logger.info(f"Worker: Transformação concluída. Pedidos: {df_pedidos.shape}, Itens: {df_itens.shape}")
        return df_pedidos, df_itens

    except Exception as e:
        logger.error(f"Worker: Erro fatal na transformação de pedidos: {e}", exc_info=True)
        return None, None
    finally:
        if conn:
            conn.close()

# --- Função de Atualização de Estoque ---

def update_stock_worker(items_df: DataFrame, db_path: Path):
    """
    Worker para atualizar o estoque com base nos itens vendidos.
    Executa UPDATEs no banco de dados. DEVE SER EXECUTADO DE FORMA CONTROLADA.
    """
    conn = None
    try:
        conn = get_db_connection(db_path)
        cursor = conn.cursor()
        updates_done = 0
        updates_failed = 0

        logger.info(f"Worker Estoque: Atualizando estoque para {items_df.shape[0]} itens.")

        for row in items_df.to_records():
            prod_id = row.get("produto_id")
            qtd = row.get("quantidade")
            if prod_id and qtd:
                try:
                    # Usar transação para cada update ou um lote
                    cursor.execute(
                        "UPDATE produtos SET estoque = estoque - ? WHERE id = ?",
                        (int(qtd), int(prod_id))
                    )
                    # O ideal é verificar o rowcount e se o estoque não ficou negativo
                    conn.commit() # Commit por item ou por lote? Por lote é melhor.
                    updates_done += 1
                except Exception as e_upd:
                    logger.error(f"Worker Estoque: Falha ao atualizar prod_id {prod_id}: {e_upd}")
                    conn.rollback()
                    updates_failed += 1
            else:
                 logger.warning(f"Worker Estoque: Pulando linha inválida: {row}")
                 updates_failed += 1

        logger.info(f"Worker Estoque: Concluído. Sucesso: {updates_done}, Falhas: {updates_failed}")

    except Exception as e:
        logger.error(f"Worker Estoque: Erro fatal na atualização de estoque: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()