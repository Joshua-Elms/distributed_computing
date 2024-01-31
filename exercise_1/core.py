import socket
import time
import pprint
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

            case b"set":
                req_key = msg[4:end_loc-INT_SIZE-1]
                req_size = int.from_bytes(msg[end_loc-INT_SIZE:end_loc], "big")
                data_msg_partial = text_msg[end_loc+1+END_SIZE:]
                self.recv_set(req_key, req_size, data_msg_partial)
                print(self.kvstore)

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
        if response["value"] is None:
            time.sleep(SLEEPTIME)
            self.conn.sendall(b"KEY NOT FOUND " + END)

        else:
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

        value = b"".join(data).rstrip(b" " + END)
        status = self.kvstore.set(key, value, len(
            value).to_bytes(INT_SIZE, byteorder=INT_ORDER))
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
                print(f"CLIENT: Server disconnected")
                break

        header = b"".join(header)
        end_loc = header.find(END) - 1
        if header == b"KEY NOT FOUND " + END:
            return header, b"", b""

        header = header[0:end_loc]
        size = int.from_bytes(
            header[end_loc-INT_SIZE:end_loc], "big") + 1 + END_SIZE
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
        assert 1 <= len(key) <= KEY_SIZE, ValueError(
            f"Key length must be between 1-{KEY_SIZE} bytes, key of size {len(key)}b was passed")
        assert isinstance(key, bytes), TypeError(
            f"Key must be of type bytes, not: {type(key)}")
        assert isinstance(value, bytes), TypeError(
            f"Value must be of type bytes, not: {type(value)}")

        key = key.ljust(KEY_SIZE, b" ")
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
        size = (len(value) + 1 + END_SIZE).to_bytes(INT_SIZE, 'big')
        msg_parts = (b'set', key, size, END)
        msg = b" ".join(msg_parts)
        return msg

    def close(self):
        self.s.close()


class KVStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.touch(exist_ok=True)
        self.tmp_path = self.path.with_suffix(".tmp")

    def get(self, key):
        with self.path.open("rb") as f:
            value = None
            size = None
            start_pos = None
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
                if current_key == key:
                    value = tmp_value
                    size = tmp_size
                    start_pos = end_pos - (KEY_SIZE + INT_SIZE + tmp_size_int)
                    break

        return dict(key=key, value=value, size=size, start_pos=start_pos, end_pos=end_pos)

    def set(self, key: bytes, value: bytes, size: bytes):
        """
        delete default value for size
        """
        # will be replaced by actual size once this code goes into core.py
        key_response = self.get(key)
        # used to determine whether to rewrite or append to file
        key_exists = key_response["value"] is not None

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
            print(e)
            status = b"NOT STORED " + END

        return status

    def _rewrite(self, key: bytes, value: bytes, size: bytes, start_pos: int, end_pos: int):
        # copy to temp file so that we can overwrite lines from the original file
        shutil.copy(self.path, self.tmp_path)
        with self.tmp_path.open("rb") as f_read:
            with self.path.open("r+b") as f_write:
                f_read.seek(end_pos)
                f_write.seek(start_pos)
                f_write.truncate()  # clear file from pos to end
                while True:
                    this_key = f_read.read(KEY_SIZE)
                    if not this_key:
                        break
                    this_size = f_read.read(INT_SIZE)
                    this_size_int = int.from_bytes(
                        this_size, byteorder=INT_ORDER)
                    this_value = f_read.read(this_size_int)
                    f_write.write(this_key + this_size + this_value)

                f_write.write(key + size + value)

        self.tmp_path.unlink()

    def __str__(self):
        output = f"Key-Value store at {self.path}\n{'-'*30}\n"

        lines = []
        max_int_digits = 0
        empty = True
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

        for line in lines:
            output += f"{line[0]} {str(line[1]).rjust(max_int_digits)}b: {line[2]}\n"

        if empty:
            output += "Empty"

        output += "\n"

        return output


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
