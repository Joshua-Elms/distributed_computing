import socket
import sys

if len(sys.argv) > 1: # if interactive mode is specified in command line
    INTERACTIVE = bool(int(sys.argv[1])) 

else: # default to non-interactive mode
    INTERACTIVE = False

HOST = "127.0.0.1"
PORT = 65000

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))
s.sendall((1000000000).to_bytes(4, byteorder="big"))
data = s.recv(1024)
s.close()
print(f"\nCLIENT: Received {int.from_bytes(data, byteorder='big')}")