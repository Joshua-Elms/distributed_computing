from core import Server

serv = Server(
    HOST="127.0.0.1",
    PORT=65000,
    timeout=120,
    backlog=10,
    kvstore_path="kvstore1.data",
)

print(f"SERVER: I am about to begin listening")
serv.listen()