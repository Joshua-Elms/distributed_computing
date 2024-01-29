from core import Server
import time

serv = Server(
    HOST="127.0.0.1",
    PORT=65000,
    timeout=120,
    backlog=10,
    kvstore_path="exercise_1/kvstore1.data",
)

print(f"SERVER: I am about to begin listening")
serv.listen()
print(f"SERVER: I am done listening")

serv.close()
