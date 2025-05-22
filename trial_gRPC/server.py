from concurrent import futures
from threading import Thread

import time

import grpc

import demo_pb2
import demo_pb2_grpc

__all__ = "DemoServer"
SERVER_ADDRESS = "localhost:23333"
SERVER_ID = 1


class DemoServer(demo_pb2_grpc.GRPCDemoServicer):

    def StreamData(self, request_iterator, context):
        print("StreamData started...")

        for data in request_iterator:
            print(f"Received id={data.id:04d}, payload={data.payload}")
            # AQUI VAI PROCESSAR OS DADOS (ALOCAR PARA O PIPELINE)
            # cada instância de server pega dado de um cliente por vez, por isso é bom fazer com mais workers(threads)
            # se N clientes estão mandando mensagem sem parar e você tem menos de N workers alguns clientes vao ficar de fora
            # para solucionar da pra usar mais de um servidor ou aumentar o número de workers
                  
            time.sleep(0.1)

        print("StreamData ended.")
        return demo_pb2.Ack(message="Stream completed.")

def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))

    demo_pb2_grpc.add_GRPCDemoServicer_to_server(DemoServer(), server)

    server.add_insecure_port(SERVER_ADDRESS)
    print("------------------start Python GRPC server")
    server.start()
    server.wait_for_termination()
    time.sleep(30)
    server.stop()

    # If raise Error:
    #   AttributeError: '_Server' object has no attribute 'wait_for_termination'
    # You can use the following code instead:
    # import time
    # while 1:
    #     time.sleep(10)


if __name__ == "__main__":
    main()
