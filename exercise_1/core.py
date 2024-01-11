import socket
import sys
import time
import pprint

class Server:
    def __init__(self, HOST, PORT, timeout, backlog, kvstore_path):
        self.HOST = HOST # localhost
        self.PORT = PORT # port to listen on
        self.timeout = timeout # number of seconds to wait for client connection before timing out
        self.backlog = backlog # number of queued connections allowed before refusing new connections
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # create a socket object, different protocols could be used
        self.s.bind((self.HOST, self.PORT))
        self.s.settimeout(self.timeout) 
        self.kvstore = KVStore(kvstore_path) # initialize kvstore
        self.conn = None # initialized in self.listen()
        self.addr = None # initialized in self.listen()
        
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
                self.recv()
                
    def recv(self):
        """
        Receive GET/SET from client, pass to appropriate function
        """
        request = self.conn.recv(1024)
        
        match request:
            case b"GET":
                print(f"SERVER: Received {request.decode()}")
                self.kvstore.display()
                self.recv_get()
                
            case b"SET":
                print(f"SERVER: Received {request.decode()}")
                self.kvstore.display()
                self.recv_set()
                
            case b"":
                None
                
            case _:
                print(f"SERVER: Received invalid request: {request}")
        
                
    def recv_get(self):
        """
        Receive a GET from client, get from kvstore, and return to client
        """
        key = self.conn.recv(1024)
        value = self.kvstore.get(key)
        size = len(value)
        header = f"VALUE {key} {size}\r\n".encode('utf-8')
        msg = f"{value}\r\n".encode('utf-8')
        self.conn.sendall(header)
        self.conn.sendall(msg)
        self.conn.sendall(b"END\r\n")
        
    def recv_set(self):
        """
        Receive a SET from client, parse, set in kvstore, and return status to client
        """
        msg = self.conn.recv(1024).decode('utf-8')
        key, value = self.parse_set_msg(msg)
        status = self.kvstore.set(key, value)
        self.conn.sendall(status)
        
    def parse_set_msg(self, msg: str):
        """
        Parse a SET from client, depends on format specified by client.set_msg()
        """
        key, value = msg.split(" ", maxsplit=1)
        value = value.rstrip("\r\n")
        return key.encode("utf-8"), value.encode("utf-8")
        
    def close(self):
        self.s.close()
        
        
class Client:
    def __init__(self, HOST, PORT, connection_timeout=60):
        self.HOST = HOST # localhost
        self.PORT = PORT # port to listen on
        self.connection_timeout = connection_timeout # number of seconds to wait for client connection before timing out
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # create a socket object, different protocols could be used
        start = time.time()
        while start + self.connection_timeout > time.time():
            try:
                self.s.connect((HOST, PORT)) # connect this socket to established server
                break
            except ConnectionRefusedError as e: # if server is not listening, wait 5 seconds and try again
                if start + self.connection_timeout > time.time():
                    print(f"CLIENT: Connection refused by server at {HOST}:{PORT}, trying again")
                    time.sleep(5)
                    
                else:
                    print(f"CLIENT: Connection refused by server at {HOST}:{PORT}, exiting")
                    quit()
            
    def get(self, key):
        """
        Send a GET message to server, return header, msg, and end from server
        """
        self.s.sendall(b"GET") # send a GET message to server
        time.sleep(0.01)
        self.s.sendall(key.encode('utf-8')) 
        # receive components of the response
        header = self.s.recv(1024)
        msg = self.s.recv(1024)
        end = self.s.recv(5)
        return (header, msg, end)
        
    def set(self, key, value):
        """
        Send a SET message to server, return status message from server
        """
        self.s.sendall(b"SET") # send a SET message to server
        time.sleep(0.01)
        msg = self.set_msg(key, value)
        self.s.sendall(msg)
        response = self.s.recv(1024)
        return response
        
    def set_msg(_, key, value):
        """
        Format a SET message to send to server
        """
        return f"{key} {value}\r\n".encode('utf-8')
    
    def close(self):
        self.s.close()
        
    
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
            print(f"Im setting the key as {key!r}")
            self.store[key] = value
            status = b"STORED\r\n"
        except KeyError as e:
            status = b"NOT-STORED\r\n"
            
        return status
    
    def __str__(self):
        return str(self.store)
    
    def display(self):
        print("SERVER: KVStore state\n")
        pprint.pprint(self.store, depth=2)
        print("\n")
        
        
# s.sendall((1000000000).to_bytes(4, byteorder="big"))
# print(f"\nCLIENT: Received {int.from_bytes(data, byteorder='big')}")

def main():
    pass

if __name__ == "__main__":
    main(
        HOST = "127.0.0.1",
        PORT = 65000,
        interactive = False,
        timeout = 10,
        backlog = 10
    )