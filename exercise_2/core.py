from pathlib import Path
import socket

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
        parts = msg.split("-")
        self.PID = int(parts[0])
        self.TS = int(parts[1])
        self.body = "-".join(parts[2])  # body could include dashes

    def __lt__(self, other: "Message"):
        if self.TS < other.TS:
            return True
        elif self.TS == other.TS:
            print(
                f"TS are equal, comparing PIDs: {self.PID} < {other.PID} = {self.PID < other.PID}")
            return self.PID < other.PID
        else:
            return False


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
        newline = "\n"
        for i, msg in enumerate(self.queue):
            s += f"Queue[{i}]:\n"
            s += f"Acks: {self.acks[f'{msg.PID}-{msg.TS}']}\n"
            s += f"PID:{msg.PID}-TS:{msg.TS}-{msg.body}{newline*(i != len(self.queue)-1)}\n"

        s += "-----------\n"

        return s


class Process:
    def __init__(self, config_index, config_file: Path):
        config = self.load_config(config_file)
        self.pid = config_index
        self. = config_index
        self.ts = 0
        self.party = party
        self.queue = MessageQueue()
        self.delivered_msgs = set()

    def load_config(self, config_file):
        # Load config file and return dictionary of process IDs and ports
        pass

    def logical_clock(self):
        self.ts += 1

    def broadcast(self, msg_body):
        self.logical_clock()
        msg = f"{self.pid}-{self.ts}-{msg_body}"
        self.queue.append(msg)
        self.queue.sort(key=lambda x: int(
            x.split('-')[1]))  # Sort by timestamp
        self.send_to_all(msg)

    def send_to_all(self, msg: Message):
        for process_id, port in self.config.items():
            if process_id != self.pid:
                # Send message to process_id's port
                pass

    def receive(self, msg):
        parts = msg.split('-')
        recv_pid = parts[0]
        recv_ts = int(parts[1])
        recv_body = '-'.join(parts[2:])
        self.logical_clock()
        self.pending_acks[msg].add(self.pid)
        self.broadcast(f"ack({recv_pid}-{recv_ts})")
        self.attempt_to_deliver()

    def attempt_to_deliver(self):
        head = self.queue[0]
        acks = self.pending_acks[head]
        if all(pid in acks for pid in self.party) and head not in self.delivered_msgs:
            self.delivered_msgs.add(head)
            self.queue.pop(0)


if __name__ == "__main__":
    m1 = Message(1, 2, "Hello")
    m2 = Message(2, 3, "World")
    q = MessageQueue()
    q.enqueue(m2)
    q.enqueue(m1)
    print(q)
    q.sort()
    print(q)
