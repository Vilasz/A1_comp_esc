"""The example of data transmission using gRPC in Python."""

import time

import grpc

import demo_pb2
import demo_pb2_grpc

from multiprocessing import Process

SERVER_ADDRESS = "localhost:23333"
CLIENT_ID = 1

# unary-unary(In a single call, the client can only send request once, and the server can
# only respond once.)
def send_data(client_id, n_msgs):
    print(f"[Client {client_id}]--------------Call SimpleSend Begin--------------")
    import random
    with grpc.insecure_channel("localhost:23333") as channel:
        stub = demo_pb2_grpc.GRPCDemoStub(channel)
        for i in range(n_msgs):
            print(f"[Client {client_id}] sending one list.")
            request = demo_pb2.DataMessage(
                id=client_id * 1000 + i,
                payload=f"Client {client_id}: Payload {i}",
                value_list = [random.randint(0,500) for _ in range(1000)])
            try:
                response = stub.SimpleSendData(request)
                print(f"[Client {client_id}] Server response over message {i}: {response}")
            except KeyboardInterrupt:
                print(f"[Client {client_id}] Interrupted.")
            
    print(f"[Client {client_id}]--------------Call SimpleSend Over---------------")

# client stream to server 
def stream_data_worker(client_id):
    with grpc.insecure_channel("localhost:23333") as channel:
        stub = demo_pb2_grpc.GRPCDemoStub(channel)
        
        def generate_data(client_id):
            import random
            i = 0
            print(f"[Client {client_id}] started sending data to server.")
            try:
                while True:
                # for _ in range(1):
                    print("================================================================")
                    value_list = [random.randint(0,500) for _ in range(1000)]
                    print(f"tentando enviar values: {value_list[:10]}...")  # Mostra só os primeiros
                    yield demo_pb2.DataMessage(
                        id=client_id * 1000 + i,
                        payload=f"Client {client_id}: Payload {i}",
                        value_list=value_list
                    )
                    print(f"[Client {client_id}] sent values.")
                    time.sleep(2)
                    i += 1
            except Exception as e:
                print(f"[Client {client_id}] Exception in generate_data(): {e}")

        try:
            response = stub.StreamData(generate_data(client_id))
            print(f"[Client {client_id}] Server response: {response}")
        except KeyboardInterrupt:
            print(f"[Client {client_id}] Interrupted.")

def test_streaming_client(n_clients):
    client_count = n_clients
    processes = []

    for i in range(client_count):
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
    client_count = n_clients
    n_msgs = n_msgs
    processes = []

    for i in range(client_count):
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


if __name__ == "__main__":
    # test_streaming_client(5)
    test_simplesend_client(8, 5)
