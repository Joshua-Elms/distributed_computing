import socket, time
from core import Message

def send(fake_port: int, send_to_port: int, msg: Message):
    # create a socket object, different protocols could be used
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # client will keep trying to connect to the server for connection_timeout seconds after instantiation
    start = time.time()
    while time.time() < start + 10:
        try:
            # connect this socket to established server
            sock.connect(("127.0.0.1", send_to_port))
            break

        except ConnectionRefusedError as e:  # if server is not listening, wait 5 seconds and try again
            if time.time() < start + 10:
                time.sleep(5)
                print(f"Process {50} could not connect to {send_to_port}, this whole thing is gonna blow")
                quit()

    sock.sendall(msg.encode())
    sock.close()