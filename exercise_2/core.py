from pathlib import Path
import socket
import json
import time


class Message:
    def __init__(self, PID: None, TS: None, body: None):
        """
        If an empty message is created, it is assumed that it is will be populated by decoded a recieved message
        """
        self.PID = PID
        self.TS = TS
        self.body = body

    def encode(self):
        return f"{self.PID}-{self.TS}-{self.body}".encode("utf-8")

    def decode(self, msg: bytes):
        msg = msg.decode("utf-8")
        print(msg)
        parts = msg.split("-")
        self.PID = int(parts[0])
        self.TS = int(parts[1])
        self.body = "-".join(parts[2:])  # body could include dashes
        print("Here's me: ", self)

    def __lt__(self, other: "Message"):
        if self.TS < other.TS:
            return True
        elif self.TS == other.TS:
            print(
                f"TS are equal, comparing PIDs: {self.PID} < {other.PID} = {self.PID < other.PID}")
            return self.PID < other.PID
        else:
            return False

    def __str__(self):
        return f"Message: {self.PID}-{self.TS}-{self.body}"


class MessageQueue:
    def __init__(self):
        self.queue = []
        # keys will be f"{PID}-{TS}" (sender PID and send timestamp) and values will be a set of PIDs that have acked the message
        self.acks = {}

    def enqueue(self, msg: Message):
        """
        Because enqueue will only be called upon receiving a msg which is not an ack (which only happens once), we can assume that the message is not already in the queue
        """
        self.queue.append(msg)
        self.acks[f"{msg.PID}-{msg.TS}"] = set((msg.PID,))

    def dequeue(self) -> Message:
        return self.queue.pop(0)

    def peek(self) -> Message:
        return self.queue[0]

    def sort(self):
        self.queue.sort()

    def __str__(self):
        s = "Queue state\n-----------\n"
        for i, msg in enumerate(self.queue):
            s += f"Queue[{i}]\n"
            s += f"Acks: {self.acks[f'{msg.PID}-{msg.TS}']}\n"
            s += str(msg) + "\n"*((i != len(self.queue)-1) + 1)

        if len(self.queue) == 0:
            s += "Empty\n"
        s += "-----------\n"

        return s


class Process:
    def __init__(self, config_index: int, config_file: Path, timeout: int = 120):
        config = self._load_config(config_file)
        # PID is also the port number
        self.PID = config[f"{config_index}"]["port"]
        # add all ports to party
        self.party = [port for port in [val["port"] for val in config.values()]]
        # starting timestamp doesn't matter
        self.TS = 0
        self.queue = MessageQueue()
        self.delivered_msgs = set()
        # create socket
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # allow reuse of socket
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.s.bind(("127.0.0.1", self.PID))
        self.timeout = timeout
        self.s.settimeout(self.timeout)
        self.conn = None
        self.addr = None

        self.main_loop()


    def main_loop(self):
        self.s.listen(100)
        while True:
            try:
                self.conn, self.addr = self.s.accept()
                with self.conn:
                    print(f"Connected by {self.addr}")
                    parts = []
                    while True:
                        part = self.conn.recv(1024)
                        if not part:
                            break
                        parts.append(part)
                raw_msg = b"".join(parts)
                msg = Message(None, None, None)
                msg.decode(raw_msg)
                print("Received message: ", msg)
                self._handle_receive(msg)
            except socket.timeout:
                print(f"Process {self.PID} timed out")
                break

    def _load_config(self, config_file: Path) -> dict:
        # Load config file and return dictionary of process IDs and ports
        with open(config_file, "r") as f:
            return json.load(f)

    def logical_clock(self):
        self.TS += 1

    def broadcast(self, msg_body: str):
        self.logical_clock()
        # create message to broadcast
        msg = Message(self.PID, self.TS, msg_body)
        self.queue.enqueue(msg)  # enqueue() handles self ack
        self.queue.sort()
        self._send_to_all(msg.encode())

    def _send_to_all(self, msg: Message):
        for process_id in self.party:
            if process_id != self.PID:  # don't send to self, handled in broadcast()
                self._send(process_id, msg)

    def _send(self, send_to_port: int, msg: Message):
        # create a socket object, different protocols could be used
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # client will keep trying to connect to the server for connection_timeout seconds after instantiation
        start = time.time()
        while time.time() < start + self.timeout:
            try:
                # connect this socket to established server
                sock.connect(("127.0.0.1", send_to_port))
                break

            except ConnectionRefusedError as e:  # if server is not listening, wait 5 seconds and try again
                if time.time() < start + self.timeout:
                    time.sleep(5)
                    print(f"Process {self.PID} could not connect to {send_to_port}, this whole thing is gonna blow")
                    quit()

        sock.sendall(msg.encode())
        sock.close()


    def _handle_receive(self, msg: Message):
        self.logical_clock()
        if msg.body[:3] == "ack":  # case where the message is an ack
            self.queue.acks[f"{msg.PID}-{msg.TS}"].add(msg.PID)
            self._attempt_to_deliver()  # only need to attempt msg delivery when recv an ack

        else:  # case where the message is not an ack
            self.queue.enqueue(msg)
            ack_msg = Message(self.PID, self.TS, f"ack:{msg.PID}-{msg.TS}")
            self.broadcast(ack_msg.encode())

    def _attempt_to_deliver(self):
        head = self.queue.peek()
        acks = self.queue.acks[f"{head.PID}-{head.TS}"]
        if all(pid in acks for pid in self.party):
            print(f"Process {self.PID} delivering message: {head}")
            self.queue.dequeue()
            self.delivered_msgs.add(head)  # optional, but useful for testing


if __name__ == "__main__":
    config_idx = 0
    config_file = Path("config_0.json")
    m1 = Message(1, 2, "Hello")
    m2 = Message(2, 3, "World")
    p1 = Process(config_idx, config_file)
    p1.queue.enqueue(m2)
    p1.queue.enqueue(m1)
    print(p1.queue)
    p1.queue.sort()
    print(p1.queue)
