# server.py
from concurrent import futures
import time
import grpc
import demo_pb2
import demo_pb2_grpc

# --- Database Imports and Configuration ---
import psycopg2
from psycopg2.pool import SimpleConnectionPool

# Ideally, move these to a configuration file or environment variables
DB_CONFIG = {
    "host": "localhost",
    "database": "my_etl_db",
    "user": "my_etl_user",
    "password": "your_secure_password" # CHANGEME
}
# Initialize a connection pool globally or pass it to the servicer
# For simplicity here, global. In a larger app, dependency injection is better.
db_pool = None

def init_db_pool():
    global db_pool
    try:
        db_pool = SimpleConnectionPool(minconn=1, maxconn=10, **DB_CONFIG) # Max workers for gRPC server
        # Test connection
        conn = db_pool.getconn()
        print("Successfully connected to PostgreSQL and created pool.")
        db_pool.putconn(conn)
    except Exception as e:
        print(f"FATAL: Could not connect to PostgreSQL or create pool: {e}")
        db_pool = None # Ensure it's None if init fails
        raise # Re-raise to stop server startup if DB is essential

# --- End Database Configuration ---

SERVER_ADDRESS = "localhost:23333" # Keep or make configurable
SERVER_ID = 1 # Not used much here

class DemoServer(demo_pb2_grpc.GRPCDemoServicer):
    def StreamData(self, request_iterator, context):
        client_peer = context.peer() # Get client IP:port for logging
        print(f"StreamData started for client: {client_peer}")
        
        if db_pool is None:
            msg = "Database connection pool not initialized."
            print(f"ERROR for {client_peer}: {msg}")
            context.set_details(msg)
            context.set_code(grpc.StatusCode.INTERNAL)
            return demo_pb2.Ack(message=msg)

        conn = None
        events_processed_in_stream = 0
        try:
            conn = db_pool.getconn()
            with conn.cursor() as cursor: # Use cursor context manager
                for data_message in request_iterator:
                    # Extract data (ensure fields match your .proto)
                    client_event_id = data_message.id
                    client_id = client_event_id // 1000 # Example: deriving client_id
                    payload = data_message.payload
                    values_list = list(data_message.values) # Convert repeated field to Python list
                    timestamp_emission = data_message.timestamp_emission

                    # print(f"Received from C:{client_id} EvID:{client_event_id}, TS:{timestamp_emission}, Vals:{len(values_list)}")

                    # Persist to staging table
                    sql = """
                        INSERT INTO received_events 
                        (client_event_id, client_id, payload, values_array, timestamp_emission) 
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql, (
                        client_event_id,
                        client_id,
                        payload,
                        values_list, # psycopg2 handles Python list to PG ARRAY
                        timestamp_emission # Store as BIGINT (nanoseconds)
                    ))
                    events_processed_in_stream += 1
                conn.commit() # Commit after processing all items in this client's stream
            
            success_msg = f"Stream completed. Processed {events_processed_in_stream} events from {client_peer}."
            print(success_msg)
            return demo_pb2.Ack(message=success_msg)

        except psycopg2.Error as db_err:
            if conn: conn.rollback()
            error_msg = f"Database error for {client_peer}: {db_err}"
            print(error_msg)
            context.set_details(error_msg)
            context.set_code(grpc.StatusCode.INTERNAL)
            return demo_pb2.Ack(message=f"Error: {db_err}")
        except grpc.RpcError as rpc_err: # e.g. client disconnects
             if conn: conn.rollback() # Rollback any partial DB ops for this stream
             print(f"RPC error during stream for {client_peer}: {rpc_err}")
             # No need to set context, gRPC handles it. Just re-raise or handle gracefully.
             raise # Or return an Ack if appropriate
        except Exception as e:
            if conn: conn.rollback()
            error_msg = f"Unexpected error processing stream for {client_peer}: {e}"
            print(error_msg)
            context.set_details(error_msg)
            context.set_code(grpc.StatusCode.INTERNAL)
            return demo_pb2.Ack(message=f"Error: {e}")
        finally:
            if conn and db_pool:
                db_pool.putconn(conn)
            print(f"StreamData connection released for client: {client_peer}")

def main():
    try:
        init_db_pool() # Initialize DB pool at server startup
        if db_pool is None: # Check if initialization failed
             print("Exiting server due to DB pool initialization failure.")
             return
    except Exception as e:
        print(f"Server startup failed during DB pool init: {e}")
        return

    # Adjust max_workers based on your load testing needs and server resources
    # This pool is for handling incoming gRPC requests, not for the ETL processing itself.
    grpc_server_thread_pool = futures.ThreadPoolExecutor(max_workers=20) # Increased for more clients
    
    server = grpc.server(grpc_server_thread_pool)
    demo_pb2_grpc.add_GRPCDemoServicer_to_server(DemoServer(), server)
    server.add_insecure_port(SERVER_ADDRESS)
    
    print(f"--- Python gRPC server started on {SERVER_ADDRESS} with {grpc_server_thread_pool._max_workers} workers ---")
    
    try:
        server.start()
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Server shutting down...")
        server.stop(grace=5).wait() # Allow 5 seconds for graceful shutdown
    finally:
        if db_pool:
            db_pool.closeall()
            print("Database connection pool closed.")
    print("Server stopped.")


if __name__ == "__main__":
    main()