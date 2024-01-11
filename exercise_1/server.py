import socket
import sys

HOST = "127.0.0.1"
PORT = 65000

timeout = float(sys.argv[1])
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen()
s.settimeout(timeout)
conn, addr = s.accept()
with conn:
    print(f"\nSERVER: Connected by {addr}")
    while True:
        data = conn.recv(1024)
        if not data:
            break
        conn.sendall(data)
s.close()