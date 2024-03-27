import zmq
import json
import sys
import time


class Process:
    def __init__(self, config):
        self.config = config
        self.my_id = config["my_id"]
        self.mids = config["mapper_ids"]
        self.rids = config["reducer_ids"]
        self.M = len(self.mids)
        self.R = len(self.rids)
        self.master_id = config["master_id"]
        self.input_file = config["input_file"]
        self.tmp_dir = config["tmp_dir"]
        self.output_file = config["output_file"]
        self.map_f = eval(config["map_f"])
        self.reduce_f = eval(config["reduce_f"])
        self.context = zmq.Context()

    def create_socket(self, socket_type) -> zmq.Socket:
        if socket_type == "REQ":
            return self.context.socket(zmq.REQ)
        elif socket_type == "REP":
            return self.context.socket(zmq.REP)
        else:
            raise ValueError("Invalid socket type")

    def connect(self, port):
        assert self.socket, "Socket not initialized"
        assert self.socket._type_name == "REQ", "Only REQ sockets can connect"
        self.socket.connect(f"tcp://127.0.0.1:{port}")

    def bind(self, port):
        assert self.socket, "Socket not initialized"
        assert self.socket._type_name == "REP", "Only REP sockets can bind"
        self.socket.bind(f"tcp://*:{port}")

    def clear_socket(self):
        self.socket.close()
        self.socket = None


class Master(Process):
    def __init__(self, config):
        super().__init__(config)
        self.run_mapreduce()

    def run_mapreduce(self):
        # stage 1: Master (B) sends data to Mappers (M) once they send a message (B:S M:L R:_)
        self.socket = self.create_socket("REP")
        self.bind(self.master_id)

        msgs = []
        while len(msgs) < self.M:
            msg = self.socket.recv_string()
            msgs.append(msg)
            self.socket.send_string(f"work1")

        print(
            "Received all messages from Mappers, now listening for them to finish work...")

        # stage 2: Reducers wait for Mappers to finish processing and sending data (B:_ M:_ R:_)

        # stage 3: Mappers send data to Reducers

        # stage 4: Reducers process data (B:_ M:_ R:_)

        # stage 5: Reducers write processed data to output file

        # stage 6: Reducers tell Master they are done (B:S M:_ R:L)
        msgs = []
        while len(msgs) < self.R:
            msg = self.socket.recv_string()
            msgs.append(msg)

        # stage 7: Master aggregates output files from Reducers (B:_ M:_ R:_)
        self.aggregate_output()
        self.clear_socket()


class Mapper(Process):
    def __init__(self, config):
        super().__init__(config)
        self.run_mapreduce()

    def run_mapreduce(self):
        # stage 1: Mappers (M) send online ack to Master (B) and receive data (B:S M:L R:_)
        self.socket = self.create_socket("REQ")
        time.sleep(1)
        self.connect(self.master_id)
        self.socket.send_string(f"online (M:{self.my_id})")
        data = self.socket.recv_string()

        # stage 2: Mappers process data (B:_ M:_ R:_)
        kv_pairs = self.process_data(data)

        # stage 3: Mappers send processed data to Reducers (B:_ M:L R:S)
        for rid in self.rids:
            msg = " ".join(
                [f"{k}:{v}" for k, v in kv_pairs if hash(k) % self.R == rid % self.R])
            msg = f"({msg})"
            self.socket = self.create_socket("REQ")
            self.connect(rid)
            self.socket.send_string(msg)

        # stage 4 onwards: Mappers do nothing
        self.clear_socket()

    def process_data(self, data):
        out = []
        for chunk in data.split():
            out.append(self.map_f(chunk))
        return out


class Reducer(Process):
    def __init__(self, config):
        super().__init__(config)
        self.run_mapreduce()

    def run_mapreduce(self):
        # stage 1: Reducer does nothing

        # stage 2: Reducer does nothing

        # stage 3: Mappers send data to Reducers (B:_ M:L R:S)
        self.socket = self.create_socket("REP")
        self.bind(self.my_id)
        msgs = []
        while len(msgs) < self.M:
            try:
                msg = self.socket.recv_string()
                msgs.append(msg)

            except zmq.error.ZMQError as e:
                msgs.append(msg)

        print(f"R{self.my_id}: received all messages from mappers: {msgs}")

        self.clear_socket()

        # stage 4: Reducer processes data (B:_ M:_ R:_)
        output = self.process_data(msgs)

        # stage 5: Reducer writes processed date to output file
        self.write_output(output)

        # step 6: Reducer tells Master it is done (B:S M:_ R:L)
        self.socket = self.create_socket("REQ")
        self.connect(self.master_id)
        self.socket.send_string(f"done (R:{self.my_id})")

        # stage 7: Reducer does nothing
        self.clear_socket()
