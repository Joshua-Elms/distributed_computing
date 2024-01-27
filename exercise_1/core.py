import socket
import time
import pprint

class Server:
    def __init__(self, HOST, PORT, timeout, backlog, kvstore_path):
        self.HOST = HOST # localhost
        self.PORT = PORT # port to listen on
        self.timeout = timeout # number of seconds to wait for client connection before timing out
        self.backlog = backlog # number of queued connections allowed before refusing new connections
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # create a socket object, different protocols could be used
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # allow reuse of socket
        # self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
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
            print(f"SERVER: Connected by {self.addr} at t={time.time()}")
            while True:
                self.recv()
                time.sleep(1)
                print(f"SERVER: Waiting for next request at t={time.time()}")
                
    def recv(self):
        """
        Receive GET/SET from client, pass to appropriate function
        """
        req = self.conn.recv(23) # 3 bytes for GET/SET, 4 bytes for msg size in bytes, 16 bytes for key
        req_type, req_size, req_key = self._decode_initial_msg(req)
        print(f"SERVER: Received {req_type!r}-{req_size!r}-{req_key!r} at t={time.time()}")
        
        match req_type:
            case b"GET":
                print(f"SERVER: Received {req_type} at t={time.time()}")
                self.kvstore.display()
                self.recv_get(req_key, req_size)
                
            case b"SET":
                print(f"SERVER: Received {req_type} at t={time.time()}")
                self.kvstore.display()
                self.recv_set(req_size)
                
            case "":
                self.conn.close()
                print(f"SERVER: Client disconnected at t={time.time()}")
                
            case _:
                print(f"SERVER: Received invalid request: {req} at t={time.time()}")
        
                
    def recv_get(self, key, size):
        """
        Receive a GET from client, get from kvstore, and return to client
        
        Server sends:
        header     - "VALUE <key(16b)> <size(4b)>\r\n" (28b)
        data block - "<value(size)b>\r\n" (size + 2b)
        end        - "END\r\n" (5b)
        """
        value = self.kvstore.get(key, size)
        size = len(value)
        header = f"VALUE {key} {size}\r\n".encode('utf-8')
        msg = f"{value}\r\n".encode('utf-8')
        print(f"SERVER: Sending {header = } to client at t={time.time()}")
        self.conn.sendall(header)
        print(f"SERVER: Sending {msg = } to client at t={time.time()}")
        self.conn.sendall(msg)
        time.sleep(1)
        print(f"SERVER: Sending END to client at t={time.time()}")
        self.conn.sendall(b"END\r\n")
        print("SERVER: GET complete")

        
    def recv_set(self):
        """
        Receive a SET from client, parse, set in kvstore, and return status to client
        """
        print(f"SERVER: Waiting for SET message from client at t={time.time()}")
        msg = self.conn.recv(1024).decode('utf-8')
        key, value = self._parse_set_msg(msg)
        status = self.kvstore.set(key, value)
        print(f"SERVER: Sending {status} to client at t={time.time()}")
        self.conn.sendall(status)
        
    def _parse_set_msg(self, msg: str):
        """
        Parse a SET from client, depends on format specified by client.set_msg()
        """
        key, value = msg.split(" ", maxsplit=1)
        value = value.rstrip("\r\n")
        return key.encode("utf-8"), value.encode("utf-8")
    
    def _decode_initial_msg(self, msg: bytes):
        """
        First message sent by client includes first 3 bytes for request type, last 4 bytes for msg size in bytes
        """
        req_type, req_size, req_key = (msg[0:3].decode("utf-8"), int.from_bytes(msg[3:7], byteorder="big"), msg[7:23].decode("utf-8"))
        return req_type, req_size, req_key
        
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
        print(f"CLIENT: Sending GET message to server at t={time.time()}")
        self.s.sendall(b"GET") # send a GET message to server
        
        print(f"CLIENT: Sending {key} to server at t={time.time()}")
        self.s.sendall(key.encode('utf-8')) 
        # receive components of the response
        print(f"CLIENT: Waiting for response from server at t={time.time()}")
        header = self.s.recv(28)
        size = int(header.decode('utf-8').split(" ")[2]) # get size of data block from header
        print(f"CLIENT: Received header from server at t={time.time()}")
        print(f"CLIENT: Header = {header!r}")
        msg = self.s.recv(size + 2) # data block size + 2 bytes for \r\n
        print(f"CLIENT: Received msg from server at t={time.time()}")
        end = self.s.recv(5)
        print(f"CLIENT: Received END from server at t={time.time()}")
        return (header, msg, end)
        
    def set(self, key, value):
        """
        Send a SET message to server, return status message from server
        """
        assert 1 <= len(key) <= 16, ValueError(f"Key must be 16 bytes or less, key of size {len(key)}b was passed")
        assert isinstance(key, bytes), TypeError(f"Key must be of type bytes, not: {type(key)}")
        
        print(f"CLIENT: Sending SET message to server at t={time.time()}")
        msg = self._set_msg(key, value)
        
        self.s.sendall(msg)
        
        response = self.s.recv(1024)
        return response
        
    def _set_msg(_, key, size):
        """
        Format a SET message to send to server
        Msg is of format "SET <size(4b)> <key(16b)>\r\n" (27b)
        """
        return f"SET {size} {key}\r\n".encode('utf-8')
    
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
            self.store[key] = value
            status = b"STORED\r\n"
        except KeyError as e:
            status = b"NOT-STORED\r\n"
            
        return status
    
    def __str__(self):
        return str(self.store)
    
    def display(self):
        pprint.pprint(self.store, depth=2)
        
        
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