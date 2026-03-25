import grpc
from concurrent import futures
import gfs_pb2
import gfs_pb2_grpc
import os
import threading
import time
from collections import *

class ChunkServer(gfs_pb2_grpc.GfsServiceServicer):
    def __init__(self):
        self.root_dir = "/tmp/gfs_data"
        if not os.path.exists(self.root_dir):
            os.makedirs(self.root_dir)
        self.pending_writes = {}
        self.writeQueue = deque()
        self.lock = threading.Lock() # Mutex for race conditions
        self.chunkList = defaultdict(list)

        self.master_ip = 'master:50051'
        self.my_ip = os.getenv('MY_IP', 'localhost:50051')

        # Background Heartbeat Thread
        threading.Thread(target=self.send_heartbeat, daemon=True).start()
        threading.Thread(target=self.commit_write, daemon=True).start()

    def send_heartbeat(self):
        while True:
            try:
                with grpc.insecure_channel(self.master_ip) as channel:
                    stub = gfs_pb2_grpc.GfsServiceStub(channel)
                    stub.Heartbeat(gfs_pb2.Empty())
            except:
                pass # Master might be down
            time.sleep(5)

    def RegisterGfs(self, request, context):
        # 1. READ
        if request.type == 2:
            return self.read_chunk(request.filename, request.offset)

        # 2. PUSH DATA (Buffer data before commit)
        if request.type == 4:
            with self.lock:
                self.pending_writes[(request.filename, request.writeId)] = request.data
            return gfs_pb2.RegistrationResponse(type=20)

        # 3. COMMIT (Primary tells replicas to save)
        if request.type == 5:
            self.writeQueue.append([request.filename, request.writeId])
            return gfs_pb2.RegistrationResponse(type=20)

        # 4. INTERNAL COMMIT (Secondary saves data)
        if request.type == 10:
            data = self.pending_writes[(request.filename, request.writeId)]
            return self.save_to_disk(request.filename, data)

        return gfs_pb2.RegistrationResponse(type=400)

    def read_chunk(self, filepath, offset):
        i = 0
        chunkList = self.chunkList[filepath]
        while i < len(chunkList) and chunkList[i] < offset:
            i += 1
        currOffset = offset - chunkList[i - 1] if i > 0 else offset

        try:
            with open(os.path.join(self.root_dir, f"{filepath}{i}.txt"), 'r') as f:
                f.seek(currOffset)
                data = f.read(1024)  # Read 1KB chunk
                return gfs_pb2.RegistrationResponse(type=20, data=data)
        except FileNotFoundError:
            return gfs_pb2.RegistrationResponse(type=404)

    def commit_write(self):
        while True:
            if self.writeQueue:
                filename, writeId = self.writeQueue.popleft()
                with grpc.insecure_channel(self.master_ip) as chan:
                    stub = gfs_pb2_grpc.GfsServiceStub(chan)
                    res = stub.RegisterGfs(gfs_pb2.GfsRequest(type=1, filename=filename))
                for replica_ip in res.chunkServers:
                    with grpc.insecure_channel(replica_ip) as chan:
                        stub = gfs_pb2_grpc.GfsServiceStub(chan)
                        res = stub.RegisterGfs(gfs_pb2.GfsRequest(type=10, filename=filename, writeId = writeId))
                data = self.pending_writes[(filename, writeId)]
                self.save_to_disk(filename, data)


    def save_to_disk(self, filename, data):
        currChunk = len (self.chunkList[filename]) if filename in self.chunkList else 0
        currLastOff = self.chunkList[filename][-1] if filename in self.chunkList else 0
        currLastOff+=len(data)
        filepath = os.path.join(self.root_dir, f"{filename}{currChunk}.txt")
        with open(filepath, 'w') as f:
            f.write(data)
        self.chunkList[filename].append(currLastOff)
        return gfs_pb2.RegistrationResponse(type=200)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gfs_pb2_grpc.add_GfsServiceServicer_to_server(ChunkServer(), server)
    server.add_insecure_port('[::]:50051')
    print(f"ChunkServer started on {os.getenv('MY_IP')}...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()