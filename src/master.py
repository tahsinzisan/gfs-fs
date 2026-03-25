import grpc
from concurrent import futures
import gfs_pb2
import gfs_pb2_grpc
import heapq
import mysql.connector
import json
import os
import time
import threading

# DB Configuration
DB_CONFIG = {
    'user': 'root', 'password': 'rootpassword',
    'host': 'mysql_db', 'database': 'gfs_metadata'
}

class GfsMaster(gfs_pb2_grpc.GfsServiceServicer):
    def __init__(self):
        self.loadHeap = []
        self.known_servers = [f"chunkserver_{i}:50051" for i in range(1, 4)]
        self.writeId = 0

        # NEW: Health Tracking
        self.server_health = {} # { "ip:port": last_heartbeat_timestamp }
        self.HEALTH_THRESHOLD = 15 # Seconds before a server is "dead"

        for srv in self.known_servers:
            heapq.heappush(self.loadHeap, (0, srv))
            self.server_health[srv] = time.time() # Initially assume healthy

        # NEW: Background thread to prune dead nodes
        threading.Thread(target=self._monitor_health, daemon=True).start()

        self._init_db()

    def Heartbeat(self, request, context):
        peer = context.peer().replace("ipv4:", "").replace("ipv6:", "")
        for srv in self.known_servers:
            if srv.split(':')[0] in peer:
                self.server_health[srv] = time.time()
                break

        return gfs_pb2.Empty()

    def _monitor_health(self):
        """Background loop to remove dead servers from the selection pool."""
        while True:
            now = time.time()
            alive_servers = []

            # Check all known servers
            for srv, last_seen in self.server_health.items():
                if now - last_seen < self.HEALTH_THRESHOLD:
                    alive_servers.append(srv)
                else:
                    print(f"Master: ALERT! {srv} is down. Removing from cluster.")

            # Re-build the load heap with only healthy servers
            # This is where 'Fault Tolerance' actually happens
            new_heap = []
            for load, srv in self.loadHeap:
                if srv in alive_servers:
                    heapq.heappush(new_heap, (load, srv))

            self.loadHeap = new_heap
            time.sleep(5) # Run check every 5 seconds

    def _init_db(self):
        """Initialize MySQL table for file metadata persistence."""
        retries = 5
        while retries > 0:
            try:
                self.conn = mysql.connector.connect(**DB_CONFIG)
                self.cursor = self.conn.cursor()
                self.cursor.execute("""
                                    CREATE TABLE IF NOT EXISTS files (
                                                                         filename VARCHAR(255) PRIMARY KEY,
                                        chunks JSON,
                                        primary_srv VARCHAR(255)
                                        )
                                    """)
                self.conn.commit()
                return
            except Exception as e:
                print(f"DB Connection failed, retrying... ({e})")
                time.sleep(5)
                retries -= 1



    def RegisterGfs(self, request, context):
        try:
            if request.type == 1: # READ REQUEST
                return self.handle_read(request.filename)
            elif request.type == 3: # WRITE REQUEST
                return self.handle_write(request.filename, request.data)
        except Exception as e:
            print(f"Error processing request: {e}")
        return gfs_pb2.RegistrationResponse(type=500)

    def handle_read(self, filename):
        self.cursor.execute("SELECT chunks FROM files WHERE filename = %s", (filename,))
        row = self.cursor.fetchone()
        if row:
            chunks = json.loads(row[0])
            return gfs_pb2.RegistrationResponse(type=10, chunkServers=chunks)
        return gfs_pb2.RegistrationResponse(type=404)

    def handle_write(self, filename, data):
        self.writeId+=1
        # Check if file exists
        self.cursor.execute("SELECT chunks, primary_srv FROM files WHERE filename = %s", (filename,))
        row = self.cursor.fetchone()

        if row:

            return gfs_pb2.RegistrationResponse(
                type=10, chunkServers=json.loads(row[0]), primary=row[1], writeId=self.writeId
            )

        # Allocate new chunks (Replication Factor: 3)
        size = len(data) if data else 0
        chunks = []
        temp_heap = []

        # Pick top 3 servers with lowest load
        count = 0
        while self.loadHeap and count < 3:
            load, srv = heapq.heappop(self.loadHeap)
            chunks.append(srv)
            temp_heap.append((load + size, srv))
            count += 1

        # Restore heap
        for item in temp_heap:
            heapq.heappush(self.loadHeap, item)

        primary = chunks[0]

        # Persist to MySQL
        self.cursor.execute(
            "INSERT INTO files (filename, chunks, primary_srv) VALUES (%s, %s, %s)",
            (filename, json.dumps(chunks), primary)
        )
        self.conn.commit()

        return gfs_pb2.RegistrationResponse(
            type=10, chunkServers=chunks, primary=primary, writeId = self.writeId
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gfs_pb2_grpc.add_GfsServiceServicer_to_server(GfsMaster(), server)
    server.add_insecure_port('[::]:50051')
    print("GFS Master running on port 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()