from core import *
import json
import sys

server_info = json.loads(sys.argv[1])
HOST = server_info["HOST"]
PORT = int(server_info["PORT"])
timeout = int(server_info["timeout"])
backlog = int(server_info["backlog"])
kvstore_path = server_info["kvstore_path"]

serv = Server(
    HOST=HOST,
    PORT=PORT,
    timeout=timeout,
    backlog=backlog,
    kvstore_path=kvstore_path,
)

print(f"Server: Listening on {HOST}:{PORT}...")
serv.listen()
print(f"Server: Detached from {HOST}:{PORT}")
serv.close()
