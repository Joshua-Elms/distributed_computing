import socket
import time
import pprint

class KVStore:
    def __init__(self, path):
        self.store = {}
        self.path = path
        
    def get(self, key):
        try:
            return self.store[key]
        except KeyError as e:
            return b"KEY NOT FOUND\r\n"
        
    def set(self, key, value):
        try:
            self.store[key] = value
            status = b"STORED\r\n"
        except KeyError as e:
            status = b"NOT-STORED\r\n"
            
        return status
    
    def __str__(self):
        return str(self.store)
    
    def display(self):
        pprint.pprint(self.store, depth=2)

HOST = "127.0.0.1"
PORT = 65000
timeout = 30
backlog = 10
kvstore_path = "exercise_1/kvstore1.data"
END = br"\r\n"

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # create a socket object, different protocols could be used
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # allow reuse of socket
s.settimeout(timeout) 
kvstore = KVStore(kvstore_path) # initialize kvstore

s.bind((HOST, PORT))
s.listen()
conn, addr = s.accept()
with conn:
    print(f"Connected by {addr}")
    fragments = []
    while True:
        # expecting text line to come first, reading in all data to a buffer until \r\n detected
        chunk = conn.recv(256)
        fragments.append(chunk)
        if END in chunk:
            break
        # req_type = data[0:3]
        # req_size = int.from_bytes(data[3:7])
        # req_key = data[7:23]
        # print(f"Received {req_type!r}-{req_size!r}-{req_key!r}", byteorder="big")
        # if not data:
        #     break

    data = b"".join(fragments)
    end_loc = data.find(END) - 1
    msg = data[0:end_loc]
    req_type = msg[0:3]
    req_key = msg[4:8]
    req_size = int.from_bytes(msg[9:end_loc], "big")
    print(f"Received: \"{req_type.decode('utf-8')} {req_size} {req_key.decode('utf-8')} {END.decode('utf-8')}\"")
    conn.sendall(b"OK " + END)
    print(f"Sent ack")

s.close()