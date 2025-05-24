"""The example of data transmission using gRPC in Python."""

import time

import grpc

import demo_pb2
import demo_pb2_grpc

from multiprocessing import Process

__all__ = [
    "simple_method",
    "client_streaming_method",
    "server_streaming_method",
    "bidirectional_streaming_method",
    "stream_data_method"
]

SERVER_ADDRESS = "localhost:23333"
CLIENT_ID = 1

# unary-unary(In a single call, the client can only send request once, and the server can
# only respond once.)
def simple_method(stub):
    print("--------------Call SimpleMethod Begin--------------")
    request = demo_pb2.Request(
        client_id=CLIENT_ID, request_data="called by Python client"
    )
    response = stub.SimpleMethod(request)
    print(
        "resp from server(%d), the message=%s"
        % (response.server_id, response.response_data)
    )
    print("--------------Call SimpleMethod Over---------------")


# stream-unary (In a single call, the client can transfer data to the server several times,
# but the server can only return a response once.)
def client_streaming_method(stub):
    print("--------------Call ClientStreamingMethod Begin--------------")

    # create a generator
    def request_messages():
        for i in range(5):
            request = demo_pb2.Request(
                client_id=CLIENT_ID,
                request_data="called by Python client, message:%d" % i,
            )
            yield request

    response = stub.ClientStreamingMethod(request_messages())
    print(
        "resp from server(%d), the message=%s"
        % (response.server_id, response.response_data)
    )
    print("--------------Call ClientStreamingMethod Over---------------")


# unary-stream (In a single call, the client can only transmit data to the server at one time,
# but the server can return the response many times.)
def server_streaming_method(stub):
    print("--------------Call ServerStreamingMethod Begin--------------")
    request = demo_pb2.Request(
        client_id=CLIENT_ID, request_data="called by Python client"
    )
    response_iterator = stub.ServerStreamingMethod(request)
    for response in response_iterator:
        print(
            "recv from server(%d), message=%s"
            % (response.server_id, response.response_data)
        )

    print("--------------Call ServerStreamingMethod Over---------------")


# stream-stream (In a single call, both client and server can send and receive data
# to each other multiple times.)
def bidirectional_streaming_method(stub):
    print(
        "--------------Call BidirectionalStreamingMethod Begin---------------"
    )

    # create a generator
    def request_messages():
        for i in range(5):
            request = demo_pb2.Request(
                client_id=CLIENT_ID,
                request_data="called by Python client, message: %d" % i,
            )
            yield request
            time.sleep(1)

    response_iterator = stub.BidirectionalStreamingMethod(request_messages())
    for response in response_iterator:
        print(
            "recv from server(%d), message=%s"
            % (response.server_id, response.response_data)
        )

    print("--------------Call BidirectionalStreamingMethod Over---------------")

# client stream to server 
def stream_data_worker(client_id):
    with grpc.insecure_channel("localhost:23333") as channel:
        stub = demo_pb2_grpc.GRPCDemoStub(channel)

        # # gererates simpler data for test
        # def generate_data():
        #     i = 0
        #     print(f"[Client {client_id}] started sending data to server {channel}|{stub}.")
        #     while True:
        #         yield demo_pb2.DataMessage(id=client_id * 1000 + i, payload=f"Client {client_id}: Payload {i}")
        #         print(f"[Client {client_id}] sent payload: {i}")
        #         time.sleep(0.5)
        #         i += 1
        
        def generate_data(client_id):
            import random
            i = 0
            print(f"[Client {client_id}] started sending data to server.")
            try:
                while True:
                # for _ in range(1):
                    print("================================================================")
                    values = [random.randint(0,600) for _ in range(1000)]
                    print(f"tentando enviar values: {values[:10]}...")  # Mostra só os primeiros
                    yield demo_pb2.DataMessage(
                        id=client_id * 1000 + i,
                        payload=f"Client {client_id}: Payload {i}",
                        values=values
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

def main():
    client_count = 5
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


if __name__ == "__main__":
    main()
