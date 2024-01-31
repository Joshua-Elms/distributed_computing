"""
This file contains three classes: Server, Client, and KVStore

Server is a class that listens for connections from clients and dispatches requests to the appropriate function.

Client is a class that connects to a server and sends requests to the server.

KVStore is a class that is used by the Server class to store key-value pairs. It is a simple file-based key-value store.

See the docstrings of each class for more information, or the README.pdf for a high-level overview of the project.

7 constants are defined at the top of the file:
- END: The end of a message. Only certain processes use this to delimit messages. 
- END_SIZE: The size of the END constant. 
- BUF_SIZE: The size (in bytes) of the buffer used to receive data from the network.
- SLEEPTIME: The amount of time to sleep between before the server or client sends or receives data.
- INT_SIZE: The size of an integer (in bytes). Used both in messages and in KVStore.
- INT_ORDER: The byte order of integers. 
- KEY_SIZE: The size of a key (in bytes). Used both in messages and in KVStore.

INT_SIZE, INT_ORDER, and KEY_SIZE cannot be changed once a KVStore has been created; the layout of the KVStore file is dependent on these constants.
"""

import socket
import time
from pathlib import Path
import shutil

END = b"\r\n"
END_SIZE = len(END)
BUF_SIZE = 4096
SLEEPTIME = 0.05
INT_SIZE = 4
KEY_SIZE = 10
INT_ORDER = "big"


class Server:
    """
    Class that listens for connections from clients and dispatches requests to the appropriate function.

    Requires the following parameters:
    - HOST: The IP address of the server.
    - PORT: The port that the server listens on.
    - timeout: The number of seconds to wait for a client connection before timing out.
    - backlog: The number of queued connections allowed before refusing new connections.
    - kvstore_path: The path to the file used to store key-value pairs.
    - vocal: Whether to print information about the server's state to the console.
    """

    def __init__(self, HOST: int | str, PORT: int, timeout: int, backlog: int, kvstore_path: str, vocal: bool = True) -> None:
        self.HOST = HOST
        self.PORT = PORT
        self.timeout = timeout
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
        self.vocal = vocal

    def listen(self):
        """
        Main event loop for server. Listens for connections from clients and dispatches requests to the appropriate function.
        """
        # allow server to accept up to self.backlog connections
        self.s.listen(self.backlog)

        # main event loop
        while True:
            # self.conn is a new socket object usable to send and receive data on the connection
            self.conn, self.addr = self.s.accept()
            # context management for self.conn; closes socket when block is exited or vice versa
            with self.conn:
                if self.vocal:
                    print(f"SERVER: Connected by {self.addr}")
                fragments = []
                while True:
                    # slight delay to allow message time to arrive
                    time.sleep(SLEEPTIME)
                    # receive up to BUF_SIZE bytes from client
                    chunk = self.conn.recv(BUF_SIZE)
                    fragments.append(chunk)
                    if END in chunk:  # end of message
                        self.dispatch(fragments)
                        fragments = []

                    elif not chunk:  # client disconnected
                        if self.vocal:
                            print(f"SERVER: Client disconnected")
                        break

    def dispatch(self, fragments: list) -> None:
        """
        Parse requests from client and dispatch to appropriate function.
        """
        # join fragments into a single message and extract request type
        text_msg = b"".join(fragments)
        end_loc = text_msg.find(END) - 1
        msg = text_msg[0:end_loc]
        req_type = msg[0:3]

        match req_type:
            case b"get":
                # different from set because we don't need to parse the size of the value
                req_key = msg[4:end_loc]
                self.recv_get(req_key)

            case b"set":
                req_key = msg[4:end_loc-INT_SIZE-1]
                req_size = int.from_bytes(msg[end_loc-INT_SIZE:end_loc], "big")
                data_msg_partial = text_msg[end_loc+1+END_SIZE:]
                self.recv_set(req_key, req_size, data_msg_partial)

            case _:
                print(f"SERVER: Invalid request: {msg}")

    def recv_get(self, key: bytes) -> None:
        """
        Receive a get request from client, get from kvstore, and return to client.

        If the key is not found, the server sends b"KEY NOT FOUND {END}" to the client.

        Server sends:
        header     - b"VALUE <key(1-250b)> <size(4b)> {END}"
        data block - b"<value(size)b> {END}"
        end        - b"END {END}"
        """
        response = self.kvstore.get(key)
        if response["value"] is None:  # key not found
            time.sleep(SLEEPTIME)
            self.conn.sendall(b"KEY NOT FOUND " + END)

        else:
            # response includes: key, value, size, start_pos, end_pos
            value, size = response["value"], response["size"]
            text_msg = b" ".join(
                (b"VALUE", key, size, END))
            data_msg = value + b" " + END

            time.sleep(SLEEPTIME)
            self.conn.sendall(text_msg)

            time.sleep(SLEEPTIME)
            self.conn.sendall(data_msg)

            time.sleep(SLEEPTIME)
            self.conn.sendall(b"END " + END)

    def recv_set(self, key: bytes, size: int, data_msg_partial: bytes = b"") -> None:
        """
        Upon execution, prepares to receive the data block of a set request from the client. 
        Once the data block is received, it is passed to the kvstore to be set.
        The server responds with the status of the kvstore.set operation.
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

        value = b"".join(data).rstrip(b" " + END)
        status = self.kvstore.set(key, value, len(
            value).to_bytes(INT_SIZE, byteorder=INT_ORDER))
        time.sleep(SLEEPTIME)
        self.conn.sendall(status)

    def close(self):
        """ 
        Terminate the server.
        """
        self.s.close()


class Client:
    """
    Class that connects to a server and sends requests to the server.

    Many Clients can connect to the same server at the same time, though they will be handled serially.

    Requires the following parameters:
    - HOST: The IP address of the server.
    - PORT: The port that the server listens on. The client will have a random port assigned upon connection.
    - connection_timeout: The number of seconds to keep trying to connect to the server before exiting.
    """

    def __init__(self, HOST: int | str, PORT: int, connection_timeout: int = 60, vocal: bool = True) -> None:
        self.HOST = HOST
        self.PORT = PORT
        self.connection_timeout = connection_timeout
        self.vocal = vocal
        # create a socket object, different protocols could be used
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # client will keep trying to connect to the server for connection_timeout seconds after instantiation
        start = time.time()
        while start + self.connection_timeout > time.time():
            try:
                # connect this socket to established server
                self.s.connect((HOST, PORT))
                break

            except ConnectionRefusedError as e:  # if server is not listening, wait 5 seconds and try again
                if start + self.connection_timeout > time.time():
                    if vocal:
                        print(
                            f"CLIENT: Unable to connect to server at {HOST}:{PORT}, trying again in 5 seconds")
                    time.sleep(5)

                else:
                    if vocal:
                        print(
                            f"CLIENT: Unable to connect to server at {HOST}:{PORT}, exiting")
                    quit()

    def get(self, key: bytes) -> (bytes, bytes, bytes):
        """
        Validate key and send a get message to the server. Return the response from the server.

        Requires the following parameters:
        - key: The key to get from the server. Must be a bytes object and have a length between 1 and KEY_SIZE bytes. Will be padded with spaces if it is less than KEY_SIZE bytes.
        """
        assert 1 <= len(key) <= KEY_SIZE, ValueError(
            f"Key length must be between 1-{KEY_SIZE} bytes, key of size {len(key)}b was passed")
        assert isinstance(key, bytes), TypeError(
            f"Key must be of type bytes, not: {type(key)}")

        key = key.ljust(KEY_SIZE, b" ")
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
                if self.vocal:
                    print(f"CLIENT: Server disconnected")
                break

        # join header fragments and extract size of value
        header = b"".join(header)
        end_loc = header.find(END) - 1
        if header == b"KEY NOT FOUND " + END:
            return header, b"", b""

        header = header[0:end_loc]
        size = int.from_bytes(
            header[end_loc-INT_SIZE:end_loc], "big") + 1 + END_SIZE
        nbytes = size

        # receive data block (value)
        data = []
        while nbytes > 0:
            partial = self.s.recv(BUF_SIZE)
            data.append(partial)
            nbytes -= len(partial)

            if not partial:
                if self.vocal:
                    print(f"CLIENT: Server disconnected")
                break

        value = b"".join(data).rstrip(b" " + END)
        end = self.s.recv(4 + len(END))

        return header, value, end

    def set(self, key: bytes, value: bytes) -> bytes:
        """
        Validate key and value, send a set message to the server, and return the response from the server.

        Requires the following parameters:
        - key: The key to set in the server. Must be a bytes object and have a length between 1 and KEY_SIZE bytes. Will be padded with spaces if it is less than KEY_SIZE bytes.
        - value: The value to set in the server. Must be a bytes object of length between 1 and 2^(INT_SIZE*8 - 1) bytes.
        """
        assert 1 <= len(key) <= KEY_SIZE, ValueError(
            f"Key length must be between 1-{KEY_SIZE} bytes, key of size {len(key)}b was passed")
        assert isinstance(key, bytes), TypeError(
            f"Key must be of type bytes, not: {type(key)}")
        assert isinstance(value, bytes), TypeError(
            f"Value must be of type bytes, not: {type(value)}")
        assert 1 <= len(value) <= 2**(INT_SIZE*8 - 1), ValueError(
            f"Value length must be between 1-{2**(INT_SIZE*8 - 1)} bytes, value of size {len(value)}b was passed")

        key = key.ljust(KEY_SIZE, b" ")
        text_msg = self._set_msg(key, value)
        data_msg = value + b' ' + END

        # continue trying to send message until server sends acknowledgement and status
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
                    if self.vocal:
                        print(f"CLIENT: Server disconnected")
                    break

            status = b"".join(response)
            if status not in (b"STORED " + END, b"NOT STORED " + END):
                # check to make sure server response is well-formed
                raise ValueError(
                    f"CLIENT: Server response not recognized: {status!r}")

        return status

    def _set_msg(self, key: bytes, value: bytes) -> bytes:
        """
        Format a set message to be sent to the server. Internal method, not to be used directly.
        """
        size = (len(value) + 1 + END_SIZE).to_bytes(INT_SIZE, 'big')
        msg_parts = (b'set', key, size, END)
        msg = b" ".join(msg_parts)
        return msg

    def close(self):
        """
        Close the client's connection to the server and terminate client socket.
        """
        self.s.close()


class KVStore:
    """
    A simple file-based key-value store.
    This is a bytes file; it cannot be navigated by line, only by byte.
    The first KEY_SIZE bytes of the file are the first key, followed by INT_SIZE bytes for the size of the value, followed by the value itself.
    There is no delimiter between key, size, and value; you must navigate the file by reading every key-size-value block until you read b"".
    It is not recommended to use this class directly; it is used by the Server class to store key-value pairs.

    Requires the following parameters:
    - path: The path to the file used to store key-value pairs.
    """

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.touch(exist_ok=True)
        self.tmp_path = self.path.with_suffix(".tmp")

    def get(self, key: bytes) -> dict:
        """
        Navigate the file linearly and search for the key in the file. 

        Return a dictionary with the key, value, size, start_pos, and end_pos of the key-value pair in the file.

        If the key is not found, the return dictionary's key "value" will be None.

        Requires the following parameters:
        - key: The key to search for in the file.
        """
        assert self.path.exists(), FileNotFoundError(
            f"File {self.path} has been deleted or moved.")
        with self.path.open("rb") as f:
            value = None
            size = None
            start_pos = 0
            end_pos = 0
            while True:
                current_key = f.read(KEY_SIZE)
                # if current_key is empty, we've reached the end of the file
                if not current_key:
                    break
                tmp_size = f.read(INT_SIZE)
                tmp_size_int = int.from_bytes(tmp_size, byteorder=INT_ORDER)
                end_pos += KEY_SIZE + INT_SIZE + tmp_size_int
                tmp_value = f.read(tmp_size_int)
                # this block will be executed if the key is found, and then the search loop will break
                if current_key == key:
                    value = tmp_value
                    size = tmp_size
                    start_pos = end_pos - (KEY_SIZE + INT_SIZE + tmp_size_int)
                    break

        return dict(key=key, value=value, size=size, start_pos=start_pos, end_pos=end_pos)

    def set(self, key: bytes, value: bytes, size: bytes) -> bytes:
        """
        Add or update a key-value pair in the file.

        If the key is not found in the file, the key-value pair will be appended to the end of the file.
        If the key is found in the file at position i, the key-value pairs after i will be shifted to the left and the updated key-value pair will be written to the end of the file.

        Requires the following parameters:
        - key: The key to add or update in the file.
        - value: The value to add or update in the file.
        - size: The size of the value to add or update in the file.

        Returns the status of the operation: b"STORED {END}" or b"NOT STORED {END}".
        """
        # used to determine whether to rewrite or append to file
        key_response = self.get(key)
        key_exists = key_response["value"] is not None
        assert self.path.exists(), FileNotFoundError(
            f"File {self.path} has been deleted or moved.")

        try:
            if key_exists:  # trigger rewrite, key will be overwritten at bottom of file
                if key_response["value"] != value:  # only rewrite if value is different
                    self._rewrite(
                        key, value, size, key_response["start_pos"], key_response["end_pos"])

            else:  # simply append key-size-value to bottom of file
                with self.path.open("a+b") as f:
                    line = key + size + value
                    f.write(line)

            status = b"STORED " + END

        except Exception as e:
            status = b"NOT STORED " + END

        return status

    def _rewrite(self, key: bytes, value: bytes, size: bytes, start_pos: int, end_pos: int) -> None:
        """
        Copy the file to a temporary file, read the key-value pairs from the tmp file after the pair to updated into the original file, overwriting the pair to be updated. 
        Finally, append the updated key-value pair to the end of the original file.
        """
        assert self.path.exists(), FileNotFoundError(
            f"File {self.path} has been deleted or moved.")

        try:
            # copy to temp file so that we can overwrite lines from the original file
            shutil.copy(self.path, self.tmp_path)
            # open the temp file for reading and the original file for writing
            with self.tmp_path.open("rb") as f_read:
                with self.path.open("r+b") as f_write:
                    f_read.seek(end_pos)
                    f_write.seek(start_pos)
                    f_write.truncate()  # clear file from start_pos to end
                    # read from temp file and write to original file
                    while True:
                        this_key = f_read.read(KEY_SIZE)
                        if not this_key:
                            break
                        this_size = f_read.read(INT_SIZE)
                        this_size_int = int.from_bytes(
                            this_size, byteorder=INT_ORDER)
                        this_value = f_read.read(this_size_int)
                        f_write.write(this_key + this_size + this_value)

                    # append the updated key-value pair to the end of the original file
                    f_write.write(key + size + value)

            # delete the temp file
            self.tmp_path.unlink()

        except Exception as e:
            print(f"KVSTORE: Error rewriting file: {e}")

    def __str__(self) -> str:
        assert self.path.exists(), FileNotFoundError(
            f"File {self.path} has been deleted or moved.")
        output = f"Key-Value store at {self.path}\n{'-'*30}\n"
        lines = []
        max_int_digits = 0
        empty = True
        # read the file and store the key, size, and value of each key-value pair in a list
        with self.path.open("rb") as f:
            while True:
                key = f.read(KEY_SIZE)
                if not key:
                    break
                size = f.read(INT_SIZE)
                size_int = int.from_bytes(size, byteorder=INT_ORDER)
                int_digits = len(str(size_int))
                max_int_digits = int_digits if int_digits > max_int_digits else max_int_digits
                value = f.read(size_int)
                lines.append((key, size_int, value))
                empty = False

        # format the list of key-value pairs into a string
        for line in lines:
            output += f"{line[0]} {str(line[1]).rjust(max_int_digits)}b: {line[2]}\n"

        if empty:
            output += "Empty"

        output += "\n"

        return output
