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
    CREATE TABLE IF NOT EXISTS data_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        payload TEXT,
        value_list TEXT,
        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
''')
conn.commit()

class DemoServer(demo_pb2_grpc.GRPCDemoServicer):

    def StreamData(self, request_iterator, context):
        print("StreamData started...")

        # Cria nova conexão e cursor para esta thread
        conn = sqlite3.connect("data_messages.db")
        cursor = conn.cursor()

        try:
            for data in request_iterator:
                print(f"Received id={data.id:04d}, payload={data.payload}")
                sorted_values = sorted(data.values)
                print(f"Sorted values: {sorted_values[:10]}...")

                # Converte os valores e insere
                # cursor.execute(
                #     "INSERT INTO data_messages (id, client_id, payload, value_list) VALUES (?, ?, ?, ?)",
                #     (data.id, data.id // 1000, data.payload, json.dumps(list(data.values)))
                # )
                cursor.execute(
                    "INSERT INTO data_messages (client_id, payload, value_list) VALUES (?, ?, ?)",
                    (data.id // 1000, data.payload, json.dumps(list(data.values)))
                )
                conn.commit()

        except Exception as e:
            print(f"Exception in StreamData: {e}")
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return demo_pb2.Ack(message="Erro")

        finally:
            conn.close()

        print("StreamData ended.")
        return demo_pb2.Ack(message="Stream completed.")

def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))

    demo_pb2_grpc.add_GRPCDemoServicer_to_server(DemoServer(), server)

    server.add_insecure_port(SERVER_ADDRESS)
    print("------------------start Python GRPC server")
    server.start()
    server.wait_for_termination()

    # If raise Error:
    #   AttributeError: '_Server' object has no attribute 'wait_for_termination'
    # You can use the following code instead:
    # import time
    # while 1:
    #     time.sleep(10)


if __name__ == "__main__":
    main()