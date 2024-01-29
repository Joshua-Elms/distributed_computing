import socket
import time

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 65000  # The port used by the server
END = br"\r\n"


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    key = b"key1"
    size= len(key).to_bytes(4, 'big')
    msg_parts = (b'set', key, size, END)
    msg = b" ".join(msg_parts)
    print(f"Sending {msg} to server")
    s.sendall(bytes(msg))
    while True:
        data = s.recv(1024)
        if END in data:
            break
    print(f"Received {data}")

