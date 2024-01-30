import socket
import time
import pprint
from pathlib import Path

END = br"\r\n"
BUF_SIZE = 4096
SLEEPTIME = 0.05
INT_SIZE = 4


class Server:
    def __init__(self, HOST, PORT, timeout, backlog, kvstore_path):
        self.HOST = HOST  # localhost
        self.PORT = PORT  # port to listen on
        # number of seconds to wait for client connection before timing out
        self.timeout = timeout
        # number of queued connections allowed before refusing new connections
        self.backlog = backlog
        # create a socket object, different protocols could be used
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # allow reuse of socket
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.s.bind((self.HOST, self.PORT))
        self.s.settimeout(self.timeout)
        self.kvstore = KVStore(kvstore_path)  # initialize kvstore
        self.conn = None  # initialized in self.listen()
        self.addr = None  # initialized in self.listen()

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
        while True:
            self.conn, self.addr = self.s.accept()
            with self.conn:
                print(f"SERVER: Connected by {self.addr}")
                fragments = []
                while True:
                    time.sleep(SLEEPTIME)
                    chunk = self.conn.recv(BUF_SIZE)
                    fragments.append(chunk)
                    if END in chunk:
                        self.dispatch(fragments)
                        fragments = []

                    elif not chunk:
                        print(f"SERVER: Client disconnected")
                        break

    def dispatch(self, fragments):
        """
        Receive get/set from client, pass to appropriate function
        """
        text_msg = b"".join(fragments)
        end_loc = text_msg.find(END) - 1
        msg = text_msg[0:end_loc]
        req_type = msg[0:3]
        
        match req_type:
            case b"get":
                req_key = msg[4:end_loc]
                self.recv_get(req_key)
                self.kvstore.display()

            case b"set":
                req_key = msg[4:end_loc-5]
                req_size = int.from_bytes(msg[end_loc-4:end_loc], "big")
                data_msg_partial = text_msg[end_loc+1+len(END):]
                self.recv_set(req_key, req_size, data_msg_partial)
                self.kvstore.display()

            case _:
                print(f"SERVER: This case shouldn't be used: {req_type!r}")

    def recv_get(self, key: bytes):
        """
        Receive a get request from client, get from kvstore, and return to client

        Server sends:
        header     - b"VALUE <key(1-250b)> <size(4b)> \r\n"
        data block - b"<value(size)b> \r\n"
        end        - b"END \r\n"
        """
        response = self.kvstore.get(key)
        if response == b"KEY NOT FOUND " + END:
            time.sleep(SLEEPTIME)
            self.conn.sendall(response)

        else:
            value, size = response
            text_msg = b" ".join(
                (b"VALUE", key, size.to_bytes(INT_SIZE, "big"), END))
            data_msg = value + b" " + END

            time.sleep(SLEEPTIME)
            self.conn.sendall(text_msg)

            time.sleep(SLEEPTIME)
            self.conn.sendall(data_msg)

            time.sleep(SLEEPTIME)
            self.conn.sendall(b"END " + END)

    def recv_set(self, key: bytes, size: int, data_msg_partial: bytes = b""):
        """
        Receive a SET from client, parse, set in kvstore, and return status to client
        """
        data = []
        nbytes = size
        partial_size = len(data_msg_partial)
        nbytes -= partial_size
        data.append(data_msg_partial)

        while nbytes > 0:
            partial = self.conn.recv(BUF_SIZE)
            data.append(partial)
            nbytes -= len(partial)

            if not partial:
                print(f"SERVER: Client disconnected")
                break

        data = b"".join(data).rstrip(b" " + END)
        status = self.kvstore.set(key, data)
        time.sleep(SLEEPTIME)
        self.conn.sendall(status)

    def close(self):
        self.s.close()


class Client:
    def __init__(self, HOST, PORT, connection_timeout=60):
        self.HOST = HOST  # localhost
        self.PORT = PORT  # port to listen on
        # number of seconds to wait for client connection before timing out
        self.connection_timeout = connection_timeout
        # create a socket object, different protocols could be used
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        start = time.time()
        while start + self.connection_timeout > time.time():
            try:
                # connect this socket to established server
                self.s.connect((HOST, PORT))
                break
            except ConnectionRefusedError as e:  # if server is not listening, wait 5 seconds and try again
                if start + self.connection_timeout > time.time():
                    print(
                        f"CLIENT: Connection refused by server at {HOST}:{PORT}, trying again")
                    time.sleep(5)

                else:
                    print(
                        f"CLIENT: Connection refused by server at {HOST}:{PORT}, exiting")
                    quit()

    def get(self, key: bytes):
        """
        Send a get message to server, return header, msg, and end from server
        """
        text_msg = b" ".join((b"get", key, END))
        time.sleep(SLEEPTIME)
        self.s.sendall(text_msg)
        # receive components of the response
        header = []
        while True:
            partial = self.s.recv(BUF_SIZE)
            header.append(partial)

            if END in partial:
                break

            elif not partial:
                print(f"CLIENT: Server disconnected")
                break

        header = b"".join(header)
        end_loc = header.find(END) - 1
        if header == b"KEY NOT FOUND " + END:
            return header, b"", b""

        header = header[0:end_loc]
        size = int.from_bytes(header[end_loc-4:end_loc], "big") + 1 + len(END)
        nbytes = size
        data = []
        while nbytes > 0:
            partial = self.s.recv(BUF_SIZE)
            data.append(partial)
            nbytes -= len(partial)

            if not partial:
                print(f"CLIENT: Server disconnected")
                break

        value = b"".join(data).rstrip(b" " + END)
        end = self.s.recv(4 + len(END))

        return header, value, end

    def set(self, key: bytes, value: bytes):
        """
        Send a SET message to server, return status message from server
        """
        assert 1 <= len(key) <= 250, ValueError(
            f"Key length must be between 1-250 bytes, key of size {len(key)}b was passed")
        assert isinstance(key, bytes), TypeError(
            f"Key must be of type bytes, not: {type(key)}")
        assert isinstance(value, bytes), TypeError(
            f"Value must be of type bytes, not: {type(value)}")

        text_msg = self._set_msg(key, value)
        data_msg = value + b' ' + END

        status = b""
        while not status:
            time.sleep(SLEEPTIME)
            self.s.sendall(text_msg)
            time.sleep(SLEEPTIME)
            self.s.sendall(data_msg)

            response = []
            while True:
                partial = self.s.recv(BUF_SIZE)
                response.append(partial)

                if END in partial:
                    break

                elif not partial:
                    print(f"CLIENT: Server disconnected")
                    break

            status = b"".join(response)

        return status

    def _set_msg(_, key: bytes, value: bytes):
        """
        Format a SET message to send to server
        Msg is of format b"set <size(4b)> <key(1-250b)> \r\n"
        """
        size = (len(value) + 1 + len(END)).to_bytes(INT_SIZE, 'big')
        msg_parts = (b'set', key, size, END)
        msg = b" ".join(msg_parts)
        return msg

    def close(self):
        self.s.close()


class KVStore:
    def __init__(self, path):
        self.store = {}
        self.path = Path(path)

    def get(self, key):
        try:
            value = self.store[key]
            size = len(value)
            return (self.store[key], size)
        
        except KeyError as e:
            return b"KEY NOT FOUND " + END

    def set(self, key, value):
        try:
            self.store[key] = value
            status = b"STORED " + END
        except KeyError as e:
            status = b"NOT STORED " + END

        return status

    def __str__(self):
        return str(self.store)

    def display(self):
        pprint.pprint(self.store, depth=2)


def main():
    pass


if __name__ == "__main__":
    main(
        HOST="127.0.0.1",
        PORT=65000,
        interactive=False,
        timeout=10,
        backlog=10
    )
