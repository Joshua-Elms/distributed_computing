from core import Server

serv = Server(
    HOST = "127.0.0.1",
    PORT = 65000,
    timeout = 30,
    backlog = 10,
    kvstore_path = "exercise_1/kvstore1.data",
    )

serv.listen()

serv.close()