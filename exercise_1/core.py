import socket
import sys

class Server:
    def __init__(self, HOST, PORT, timeout, backlog, kvstore_path):
        self.HOST = HOST # localhost
        self.PORT = PORT # port to listen on
        self.timeout = timeout # number of seconds to wait for client connection before timing out
        self.backlog = backlog # number of queued connections allowed before refusing new connections
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # create a socket object, different protocols could be used
        self.s.bind((self.HOST, self.PORT))
        self.s.settimeout(self.timeout) 
        self.kvstore = KVStore(kvstore_path)
        
    def listen(self):
        """
        Main event loop for server
        
        self.s.listen() allows server to accept information
        self.s.accept() waits for up to self.backlog client(s) to accept information from
        Once a client connects, self.conn and self.addr are set to the client's connection and address, respectively
        self.conn is used to communicate with the client (send/receive data)
        self.conn destroyed when client disconnects
        """
        self.s.listen(self.backlog)
        self.conn, self.addr = self.s.accept()
        with self.conn:
            print(f"\nSERVER: Connected by {self.addr}")
            while True:
                data = self.conn.recv(1024)
                if not data:
                    break
                self.conn.sendall(data)
                
    def recv_get(self):
        key = self.conn.recv(1024)
        value = self.kvstore.get(key)
        size = len(value)
        header = f"VALUE {key} {size} \r\n"
        msg = f"{value} \r\n"
        self.conn.sendall(header)
        self.conn.sendall(msg)
        self.conn.sendall("END \r\n")
        
    def recv_set(self):
        msg = self.conn.recv(1024)
        key, value = self.parse_set_msg(msg)
        status = self.kvstore.set(key, value)
        
    def parse_set_msg(self, msg):
        pass
        
    def close(self):
        self.s.close()
        
        
class Client:
    def __init__(self, HOST, PORT):
        self.HOST = HOST # localhost
        self.PORT = PORT # port to listen on
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # create a socket object, different protocols could be used
        self.s.connect((HOST, PORT))
        
    def get(self, key):
        self.s.sendall(key)
        data = self.s.recv(1024)
        return data
        
    def set(self, key, value):
        msg = self.set_msg(key, value)
        self.s.sendall(msg)
        response = self.s.recv(1024)
        return response
        
    def set_msg(self, key, value):
        pass
    
    def close(self):
        self.s.close()
        
    
class KVStore:
    def __init__(self):
        self.store = {}
        
    def get(self, key):
        try:
            return self.store[key]
        
        except KeyError as e:
            return "500"
        
    def set(self, key, value):
        try:
            self.store[key] = value
            status = "200"
        except KeyError as e:
            status = "500"
            
        return status
        
        
s.sendall((1000000000).to_bytes(4, byteorder="big"))
print(f"\nCLIENT: Received {int.from_bytes(data, byteorder='big')}")

def main():
    pass

if __name__ == "__main__":
    main(
        HOST = "127.0.0.1",
        PORT = 65000,
        interactive = False,
        timeout = "15",
        backlog = 10
    )