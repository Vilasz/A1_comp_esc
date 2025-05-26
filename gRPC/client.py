"""The example of data transmission using gRPC in Python."""
import time

import grpc

import demo_pb2
import demo_pb2_grpc

import argparse

from multiprocessing import Process

# Para gerar os dados
import random
from faker import Faker
from datetime import datetime, timedelta
from typing import List, Sequence
fk = Faker("pt_BR")

SERVER_ADDRESS = "localhost:23333"

# Constantes

PRODUCTS: List[str] = [
    "Notebook", "Mouse", "Teclado", "Smartphone", "Fone de Ouvido",
    "Monitor", "Cadeira Gamer", "Mesa para Computador", "Impressora",
    "Webcam", "HD Externo", "SSD", "Placa de Vídeo", "Memória RAM",
    "Fonte ATX", "Placa‑mãe", "Roteador Wi‑Fi", "Leitor de Cartão SD",
    "Grampeador", "Luminária de Mesa", "Estabilizador", "Suporte p/ Notebook",
    "Mousepad Gamer", "Caixa de Som Bluetooth", "Power Bank", "Scanner",
    "Projetor", "Filtro de Linha", "Cabo USB‑C",
]

PRICES = {p: round(random.uniform(20, 4000), 2) for p in PRODUCTS}

CENTERS: List[str] = [
    "São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba",
    "Porto Alegre", "Salvador", "Manaus", "Brasília", "Fortaleza", "Cuiabá",
]

CHANNELS = ["site", "app", "telefone", "loja"]

WEIGHTS = [10, 9, 8, 6, 5, 3, 2, 1, 1, 1]

# Funções para gerar dados
def _rand_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    sec = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=sec)

def generate_pedido_data(start_dt: datetime = None, end_dt: datetime = None) -> dict:
    """Gera um único pedido com dados aleatórios"""
    if start_dt is None:
        start_dt = datetime(2023, 1, 1)
    if end_dt is None:
        end_dt = datetime(2024, 1, 1)
    
    prod_idx = random.randrange(len(PRODUCTS))
    produto = PRODUCTS[prod_idx]
    quantidade = random.choices(range(1, 11), weights=WEIGHTS, k=1)[0]
    preco = PRICES[produto]
    total = round(preco * quantidade, 2)
    dt = _rand_date(start_dt, end_dt)
    
    return {
        "cliente_id": random.randint(1, 500_000),
        "produto_id": prod_idx + 1,
        "categoria_id": random.randint(1, 20),
        "produto": produto,
        "quantidade": quantidade,
        "preco_unitario": preco,
        "valor_total": total,
        "data_pedido": dt.date().isoformat(),
        "hora_pedido": dt.time().isoformat(timespec="seconds"),
        "mes": dt.month,
        "ano": dt.year,
        "canal_venda": random.choice(CHANNELS),
        "centro_logistico_mais_proximo": random.choice(CENTERS),
        "cidade_cliente": fk.city(),
        "estado_cliente": fk.estado_sigla(),
        "dias_para_entrega": random.randint(1, 10),
        "timestamp_envio" : datetime.utcnow().isoformat()
    }


# unary-unary(In a single call, the client can only send request once, and the server can
# only respond once.)
def send_data(client_id, n_msgs, server_adress=SERVER_ADDRESS):
    print(f"[Client {client_id}] Iniciando envio de {n_msgs} pedidos...")

    with grpc.insecure_channel(server_adress) as channel:
        stub = demo_pb2_grpc.GRPCDemoStub(channel)

        for i in range(n_msgs):
            pedido_data = generate_pedido_data()
            
            request = demo_pb2.PedidoMessage(
                id=client_id * 1000 + i,
                cliente_id=pedido_data["cliente_id"],
                produto_id=pedido_data["produto_id"],
                categoria_id = pedido_data["categoria_id"],
                produto = pedido_data["produto"],
                quantidade = pedido_data["quantidade"],
                preco_unitario = pedido_data["preco_unitario"],
                valor_total = pedido_data["valor_total"],
                data_pedido = pedido_data["data_pedido"],
                hora_pedido = pedido_data["hora_pedido"],
                mes = pedido_data["mes"],
                ano = pedido_data["ano"],
                canal_venda = pedido_data["canal_venda"],
                centro_logistico_mais_proximo = pedido_data["centro_logistico_mais_proximo"],
                cidade_cliente = pedido_data["cidade_cliente"],
                estado_cliente=pedido_data["estado_cliente"],
                dias_para_entrega=pedido_data["dias_para_entrega"],
                timestamp_envio=pedido_data["timestamp_envio"]
            )

            try:
                response = stub.SimpleSendData(request)
                print(f"[Client {client_id}] Pedido {client_id * 1000 + i:04d} enviado | Status: {response.message}")
            except Exception as e:
                print(f"[Client {client_id}] Erro no pedido {client_id * 1000 + i:04d}: {str(e)}")
                continue
    
    print(f"[Client {client_id}] Envio concluído")

# client stream to server 
def stream_data_worker(client_id, server_adress=SERVER_ADDRESS):
    with grpc.insecure_channel(server_adress) as channel:
        stub = demo_pb2_grpc.GRPCDemoStub(channel)
        
        def stream_data(client_id):
            i = 0
            print(f"[Client {client_id}] started sending data to server.")
            try:
                while True:
                # for _ in range(1):
                    pedido_data = generate_pedido_data()

                    yield demo_pb2.PedidoMessage(
                        id=client_id * 1000 + i,
                        cliente_id=pedido_data["cliente_id"],
                        produto_id=pedido_data["produto_id"],
                        categoria_id = pedido_data["categoria_id"],
                        produto = pedido_data["produto"],
                        quantidade = pedido_data["quantidade"],
                        preco_unitario = pedido_data["preco_unitario"],
                        valor_total = pedido_data["valor_total"],
                        data_pedido = pedido_data["data_pedido"],
                        hora_pedido = pedido_data["hora_pedido"],
                        mes = pedido_data["mes"],
                        ano = pedido_data["ano"],
                        canal_venda = pedido_data["canal_venda"],
                        centro_logistico_mais_proximo = pedido_data["centro_logistico_mais_proximo"],
                        cidade_cliente = pedido_data["cidade_cliente"],
                        estado_cliente=pedido_data["estado_cliente"],
                        dias_para_entrega=pedido_data["dias_para_entrega"],
                        timestamp_envio=pedido_data["timestamp_envio"]
                    )

                    print(f"[Client {client_id}] sent order {client_id * 1000 + i:04d}.")
                    time.sleep(2)
                    i += 1
                    
            except Exception as e:
                print(f"[Client {client_id}] Exception in generate_data(): {e}")

        try:
            response = stub.StreamData(stream_data(client_id))
            print(f"[Client {client_id}] Server response: {response}")
        except KeyboardInterrupt:
            print(f"[Client {client_id}] Interrupted.")


def send_batch_data(client_id, size_batch, n_msgs, server_adress=SERVER_ADDRESS):

    with grpc.insecure_channel(server_adress) as channel:
        stub = demo_pb2_grpc.GRPCDemoStub(channel)

        for i in range(n_msgs):
            # if i==0:
                
            print(f"[Client {client_id}] Enviando lote {i} de {size_batch} pedidos...")
            pedidos = []
            for _ in range(size_batch):
                pedido_data = generate_pedido_data()
                pedidos.append(demo_pb2.PedidoMessage(
                    id=client_id * 1000 + i,
                    cliente_id=pedido_data["cliente_id"],
                    produto_id=pedido_data["produto_id"],
                    categoria_id=pedido_data["categoria_id"],
                    produto=pedido_data["produto"],
                    quantidade=pedido_data["quantidade"],
                    preco_unitario=pedido_data["preco_unitario"],
                    valor_total=pedido_data["valor_total"],
                    data_pedido=pedido_data["data_pedido"],
                    hora_pedido=pedido_data["hora_pedido"],
                    mes=pedido_data["mes"],
                    ano=pedido_data["ano"],
                    canal_venda=pedido_data["canal_venda"],
                    centro_logistico_mais_proximo=pedido_data["centro_logistico_mais_proximo"],
                    cidade_cliente=pedido_data["cidade_cliente"],
                    estado_cliente=pedido_data["estado_cliente"],
                    dias_para_entrega=pedido_data["dias_para_entrega"],
                    timestamp_envio=pedido_data["timestamp_envio"]
                ))

            try:
                lista = demo_pb2.ListaPedidos(id=client_id, id_client=i,pedidos=pedidos)
                response = stub.EnviarPedidosEmLote(lista)
                print(f"[Client {client_id}] Resposta lote {i} do servidor: {response.message}")
            except Exception as e:
                print(f"[Client {client_id}] Erro ao enviar lote {i}: {str(e)}")

# client stream to server 
def stream_batch_worker(client_id, size_batch, server_adress=SERVER_ADDRESS):
    with grpc.insecure_channel(server_adress) as channel:
        stub = demo_pb2_grpc.GRPCDemoStub(channel)
        
        def stream_data(client_id):
            i = 0
            print(f"[Client {client_id}] started sending data to server.")
            try:
                while True:
                # for _ in range(1):
                    print(f"[Client {client_id}] Enviando lote {i} de {size_batch} pedidos...")
                    pedidos = []
                    for _ in range(size_batch):
                        pedido_data = generate_pedido_data()
                        pedidos.append(demo_pb2.PedidoMessage(
                            id=client_id * 1000 + i,
                            cliente_id=pedido_data["cliente_id"],
                            produto_id=pedido_data["produto_id"],
                            categoria_id=pedido_data["categoria_id"],
                            produto=pedido_data["produto"],
                            quantidade=pedido_data["quantidade"],
                            preco_unitario=pedido_data["preco_unitario"],
                            valor_total=pedido_data["valor_total"],
                            data_pedido=pedido_data["data_pedido"],
                            hora_pedido=pedido_data["hora_pedido"],
                            mes=pedido_data["mes"],
                            ano=pedido_data["ano"],
                            canal_venda=pedido_data["canal_venda"],
                            centro_logistico_mais_proximo=pedido_data["centro_logistico_mais_proximo"],
                            cidade_cliente=pedido_data["cidade_cliente"],
                            estado_cliente=pedido_data["estado_cliente"],
                            dias_para_entrega=pedido_data["dias_para_entrega"],
                            timestamp_envio=pedido_data["timestamp_envio"]
                        ))


                    yield demo_pb2.ListaPedidos(id= i, id_client=client_id, pedidos=pedidos)

                    print(f"[Client {client_id}] sent batch {i}.")
                    time.sleep(2)
                    i += 1
                    
            except Exception as e:
                print(f"[Client {client_id}] Exception in generate_data(): {e}")

        try:
            response = stub.StreamPedidosEmLote(stream_data(client_id))
            print(f"[Client {client_id}] Server response: {response}")
        except KeyboardInterrupt:
            print(f"[Client {client_id}] Interrupted.")

def test_streaming_client(n_clients):
    processes = []

    for i in range(n_clients):
        p = Process(target=stream_data_worker, args=(i,))
        p.start()
        processes.append(p)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("Main process interrupted. Terminating clients...")
        for p in processes:
            p.terminate()

def test_simplesend_client(n_clients, n_msgs):
    processes = []

    for i in range(n_clients):
        p = Process(target=send_data, args=(i,n_msgs))
        p.start()
        processes.append(p)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("Main process interrupted. Terminating clients...")
        for p in processes:
            p.terminate()

def test_sendbatch_client(n_clients, size_batch, n_msgs):
    processes = []

    for i in range(n_clients):
        p = Process(target=send_batch_data, args=(i,size_batch, n_msgs))
        p.start()
        processes.append(p)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("Main process interrupted. Terminating clients...")
        for p in processes:
            p.terminate()

def test_streambatch_client(n_clients, size_batch):
    processes = []

    for i in range(n_clients):
        p = Process(target=stream_batch_worker, args=(i,size_batch))
        p.start()
        processes.append(p)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("Main process interrupted. Terminating clients...")
        for p in processes:
            p.terminate()

def main():
    parser = argparse.ArgumentParser(description='Start gRPC client.')
    parser.add_argument('--n_clients', type=int, default=1, help='Number of client processes')
    parser.add_argument('--size_batch', type=int, default=10, help='Size of each batch')
    parser.add_argument('--n_msgs', type=int, default=5, help='Number of messages/batches to send')
    parser.add_argument('--mode', choices=['simple', 'stream', 'batch', 'streambatch'], 
                       default='batch', help='Operation mode')
    
    args = parser.parse_args()
    
    if args.mode == 'simple':
        test_simplesend_client(args.n_clients, args.n_msgs)
    elif args.mode == 'stream':
        test_streaming_client(args.n_clients)
    elif args.mode == 'batch':
        test_sendbatch_client(args.n_clients, args.size_batch, args.n_msgs)
    elif args.mode == 'streambatch':
        test_streambatch_client(args.n_clients, args.size_batch)

if __name__ == "__main__":
    main()