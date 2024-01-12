from core import Server
import time

serv = Server(
    HOST = "127.0.0.1",
    PORT = 65000,
    timeout = 30,
    backlog = 10,
    kvstore_path = "exercise_1/kvstore1.data",
    )

print(f"CLIENT: I am about to begin listening at t={time.time()}")
serv.listen()
print(f"CLIENT: I am done listening at t={time.time()}")

serv.close()