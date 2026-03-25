import grpc
import gfs_pb2
import gfs_pb2_grpc
import sys

MASTER_IP = 'localhost:50051'
# When running in docker, use 'master:50051', locally 'localhost:50051'

class GFSClient:
    def __init__(self):
        self.channel = grpc.insecure_channel(MASTER_IP)
        self.stub = gfs_pb2_grpc.GfsServiceStub(self.channel)

    def read(self, filename, offset):
        # 1. Get Metadata
        meta_req = gfs_pb2.GfsRequest(type=1, filename=filename)
        meta_res = self.stub.RegisterGfs(meta_req)

        if not meta_res.chunkServers:
            print(f"File {filename} not found.")
            return

        # 2. Read from random replica (Reader)
        replica_ip = meta_res.chunkServers[0]
        # Note: In docker, this IP needs to be accessible from client
        # For this demo, we assume network bridging is handled by Docker
        print(f"Reading from {replica_ip}...")

        with grpc.insecure_channel(replica_ip) as chan:
            stub = gfs_pb2_grpc.GfsServiceStub(chan)
            res = stub.RegisterGfs(gfs_pb2.GfsRequest(type=2, filename=filename, offset=int(offset)))
            print(f"Content: {res.data}")

    def write(self, filename, data):
        # 1. Get Primary and Replicas
        meta_req = gfs_pb2.GfsRequest(type=3, filename=filename, data=data)
        meta_res = self.stub.RegisterGfs(meta_req)

        primary = meta_res.primary
        replicas = meta_res.chunkServers

        print(f"Primary: {primary}, Replicas: {replicas}")

        # 2. Push Data to ALL Replicas (Type 4)
        for ip in replicas:
            with grpc.insecure_channel(ip) as chan:
                stub = gfs_pb2_grpc.GfsServiceStub(chan)
                stub.RegisterGfs(gfs_pb2.GfsRequest(type=4, filename=filename, data=data))

        # 3. Commit via Primary (Type 5)
        with grpc.insecure_channel(primary) as chan:
            stub = gfs_pb2_grpc.GfsServiceStub(chan)
            stub.RegisterGfs(gfs_pb2.GfsRequest(type=5, filename=filename, writeId=meta_res.writeId))

        print("Write committed successfully.")

if __name__ == '__main__':
    client = GFSClient()
    cmd = sys.argv[1]
    if cmd == 'write':
        client.write(sys.argv[2], sys.argv[3])
    elif cmd == 'read':
        client.read(sys.argv[2], sys.argv[3])