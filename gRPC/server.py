from concurrent import futures
from threading import Thread

import time

import grpc
import sqlite3
import json

import demo_pb2
import demo_pb2_grpc

__all__ = "DemoServer"
SERVER_ADDRESS = "localhost:23333"
SERVER_ID = 1

# conexão com banco de dados
conn = sqlite3.connect("data_messages.db", check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS pedidos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id INTEGER,
        produto_id INTEGER,
        categoria_id INTEGER,
        produto TEXT,
        quantidade INTEGER,
        preco_unitario REAL,
        valor_total REAL,
        data_pedido DATE,
        hora_pedido TIME,
        mes INTEGER,
        ano INTEGER,
        canal_venda TEXT,
        centro_logistico_mais_proximo TEXT,
        cidade_cliente TEXT,
        estado_cliente TEXT,
        dias_para_entrega INTEGER,
        registrado_em TEXT
    )
''')
conn.commit()

class DemoServer(demo_pb2_grpc.GRPCDemoServicer):

    def SimpleSendData(self, request, context):
        print(f"Client {request.id//1000:04d} enviando pedido {request.id:04d}...")

        conn = sqlite3.connect("data_messages.db")
        cursor = conn.cursor()

        try:
            # Log básico dos dados recebidos
            print(f"""Dados recebidos - Pedido ID: {request.id:04d}
            Produto: {request.produto} (ID: {request.produto_id})
            Quantidade: {request.quantidade}
            Valor total: R${request.valor_total:.2f}""")

            # Inserção no banco de dados
            cursor.execute(
                """
                INSERT INTO pedidos (
                    cliente_id, produto_id, categoria_id, produto,
                    quantidade, preco_unitario, valor_total, data_pedido,
                    hora_pedido, mes, ano, canal_venda, 
                    centro_logistico_mais_proximo, cidade_cliente,
                    estado_cliente, dias_para_entrega
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request.cliente_id,
                    request.produto_id,
                    request.categoria_id,
                    request.produto,
                    request.quantidade,
                    request.preco_unitario,
                    request.valor_total,
                    request.data_pedido,
                    request.hora_pedido,
                    request.mes,
                    request.ano,
                    request.canal_venda,
                    request.centro_logistico_mais_proximo,
                    request.cidade_cliente,
                    request.estado_cliente,
                    request.dias_para_entrega,
                    request.timestamp_envio
                )
            )
            conn.commit()

            # Log de sucesso
            print(f"Pedido {request.id:04d} registrado com sucesso")
            return demo_pb2.Ack(
                message=f"Pedido {request.id:04d} alocado para o banco de dados",
            )

        except sqlite3.Error as e:
            print(f"Erro no banco de dados: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return demo_pb2.Ack(
                message="Erro no banco de dados",
            )

        except Exception as e:
            print(f"Erro inesperado: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return demo_pb2.Ack(
                message="Erro no processamento",
            )

        finally:
            conn.close()

    
    def StreamData(self, request_iterator, context):
        print("StreamData started...")

        # Cria nova conexão e cursor para esta thread
        conn = sqlite3.connect("data_messages.db")
        cursor = conn.cursor()

        try:
            for data in request_iterator:
                print(f"""Dados recebidos - Pedido ID: {data.id:04d}
                Produto: {data.produto} (ID: {data.produto_id})
                Quantidade: {data.quantidade}
                Valor total: R${data.valor_total:.2f}""")

                # Inserção no banco de dados
                cursor.execute(
                    """
                    INSERT INTO pedidos (
                        cliente_id, produto_id, categoria_id, produto,
                        quantidade, preco_unitario, valor_total, data_pedido,
                        hora_pedido, mes, ano, canal_venda, 
                        centro_logistico_mais_proximo, cidade_cliente,
                        estado_cliente, dias_para_entrega
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        data.cliente_id,
                        data.produto_id,
                        data.categoria_id,
                        data.produto,
                        data.quantidade,
                        data.preco_unitario,
                        data.valor_total,
                        data.data_pedido,
                        data.hora_pedido,
                        data.mes,
                        data.ano,
                        data.canal_venda,
                        data.centro_logistico_mais_proximo,
                        data.cidade_cliente,
                        data.estado_cliente,
                        data.dias_para_entrega,
                        data.timestamp_envio
                    )
                )
                conn.commit()

        except sqlite3.Error as e:
            print(f"Erro no banco de dados: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return demo_pb2.Ack(
                message="Erro no banco de dados",
            )

        except Exception as e:
            print(f"Erro inesperado: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return demo_pb2.Ack(
                message="Erro no processamento",
        )

        finally:
            conn.close()
            return demo_pb2.Ack(message=f"Stream from client completed.")
        

    def StreamPedidosEmLote(self, request_iterator, context):
        conn = sqlite3.connect("data_messages.db")
        cursor = conn.cursor()

        try:
            for data in request_iterator:
                print(f"Recebido do cliente {data.id_client} lote {data.id} com {len(data.pedidos)} pedidos")
                for pedido in data.pedidos:

                    print(f"""Dados recebidos - Pedido ID: {pedido.id:04d}
                    Produto: {pedido.produto} (ID: {pedido.produto_id})
                    Quantidade: {pedido.quantidade}
                    Valor total: R${pedido.valor_total:.2f}""")

                    cursor.execute(
                        """
                        INSERT INTO pedidos (
                            cliente_id, produto_id, categoria_id, produto,
                            quantidade, preco_unitario, valor_total, data_pedido,
                            hora_pedido, mes, ano, canal_venda, 
                            centro_logistico_mais_proximo, cidade_cliente,
                            estado_cliente, dias_para_entrega
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            pedido.cliente_id,
                            pedido.produto_id,
                            pedido.categoria_id,
                            pedido.produto,
                            pedido.quantidade,
                            pedido.preco_unitario,
                            pedido.valor_total,
                            pedido.data_pedido,
                            pedido.hora_pedido,
                            pedido.mes,
                            pedido.ano,
                            pedido.canal_venda,
                            pedido.centro_logistico_mais_proximo,
                            pedido.cidade_cliente,
                            pedido.estado_cliente,
                            pedido.dias_para_entrega,
                            pedido.timestamp_envio
                        )
                    )
                conn.commit()

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return demo_pb2.Ack(message="Erro ao salvar lote de pedidos.")

        finally:
            conn.close()    

    def EnviarPedidosEmLote(self, request, context):
        print(f"Recebido do cliente {request.id_client} lote {request.id} com {len(request.pedidos)} pedidos")

        conn = sqlite3.connect("data_messages.db")
        cursor = conn.cursor()

        try:
            for pedido in request.pedidos:
                print(f"""Dados recebidos - Pedido ID: {pedido.id:04d}
                Produto: {pedido.produto} (ID: {pedido.produto_id})
                Quantidade: {pedido.quantidade}
                Valor total: R${pedido.valor_total:.2f}""")

                cursor.execute(
                    """
                    INSERT INTO pedidos (
                        cliente_id, produto_id, categoria_id, produto,
                        quantidade, preco_unitario, valor_total, data_pedido,
                        hora_pedido, mes, ano, canal_venda, 
                        centro_logistico_mais_proximo, cidade_cliente,
                        estado_cliente, dias_para_entrega, registrado_em
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        pedido.cliente_id,
                        pedido.produto_id,
                        pedido.categoria_id,
                        pedido.produto,
                        pedido.quantidade,
                        pedido.preco_unitario,
                        pedido.valor_total,
                        pedido.data_pedido,
                        pedido.hora_pedido,
                        pedido.mes,
                        pedido.ano,
                        pedido.canal_venda,
                        pedido.centro_logistico_mais_proximo,
                        pedido.cidade_cliente,
                        pedido.estado_cliente,
                        pedido.dias_para_entrega,
                        pedido.timestamp_envio
                    )
                )
            conn.commit()
            return demo_pb2.Ack(message=f"{len(request.pedidos)} pedidos salvos com sucesso.")

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return demo_pb2.Ack(message="Erro ao salvar lote de pedidos.")

        finally:
            conn.close()



def main(n_workers):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=n_workers))

    demo_pb2_grpc.add_GRPCDemoServicer_to_server(DemoServer(), server)

    server.add_insecure_port(SERVER_ADDRESS)
    print(f"--------start Python GRPC server w/ {n_workers} workers--------")
    server.start()
    server.wait_for_termination()

    # If raise Error:
    #   AttributeError: '_Server' object has no attribute 'wait_for_termination'
    # You can use the following code instead:
    # import time
    # while 1:
    #     time.sleep(10)


if __name__ == "__main__":
    main(4)