import zmq
import json
import sys
import shutil
import time
from pathlib import Path


class Process:
    def __init__(self, config):
        self.config = config
        self.my_id = config["my_id"]
        self.mids = config["mapper_ids"]
        self.rids = config["reducer_ids"]
        self.M = len(self.mids)
        self.R = len(self.rids)
        self.master_id = config["master_id"]
        self.input_dir = Path(config["input_dir"])
        self.tmp_dir = Path(config["tmp_dir"])
        self.output_file = Path(config["output_file"])
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
        if self.tmp_dir.exists():  # delete tmp dir if exists
            shutil.rmtree(self.tmp_dir)
        if self.output_file.exists():  # delete output file if exists
            self.output_file.unlink()
        self.tmp_dir.mkdir()
        self.run_mapreduce()

    def run_mapreduce(self):
        # stage 0: Master divides input data before sending to Mappers
        data, idxs = self.chunk_input_data()
        
        # stage 1: Master (B) sends data to Mappers (M) once they send a message (B:S M:L R:_)
        self.socket = self.create_socket("REP")
        self.bind(self.master_id)

        msgs = []
        for i, idx_sublist in idxs.items():
            msg = self.socket.recv_string()
            msgs.append(msg)
            print(idxs)
            print(idx_sublist)
            print(data)
            subset = {k: data[k] for k in idx_sublist}
            self.socket.send_json(subset)

        # stage 2: Reducers wait for Mappers to finish processing and sending data (B:_ M:_ R:_)

        # stage 3: Mappers send data to Reducers

        # stage 4: Reducers process data (B:_ M:_ R:_)

        # stage 5: Reducers write processed data to output file

        # stage 6: Reducers tell Master they are done (B:S M:_ R:L)
        msgs = []
        while len(msgs) < self.R:
            try:
                msg = self.socket.recv_string()
                msgs.append(msg)
                self.socket.send_string(str(time.time()))

            except zmq.error.ZMQError as e:
                print("Error receiving message from reducer")

        # stage 7: Master aggregates output files from Reducers (B:_ M:_ R:_)
        self.aggregate_output()
        self.clear_socket()

    def aggregate_output(self):
        all_data = []
        for fpath in self.tmp_dir.glob("*.txt"):
            with open(fpath, "r") as f_in:
                data = f_in.readlines()
            
            all_data.extend(data)

        all_data = " ".join(all_data)

        with open(self.output_file, "a") as f_out:
            f_out.writelines(all_data)


    def chunk_input_data(self):
        input_data = []
        for f in self.input_dir.glob("*.txt"):
            with open(f, "r") as f_in:
                data = f_in.read()

            input_data.append(data)

        if len(input_data) == 0:
            raise ValueError("No input data found")
        
        elif len(input_data) < self.M:
            raise ValueError(f"Only {len(input_data)} input files found, rerun with {len(input_data)} mappers instead of {self.M}")
        
        chunks = {i: input_data[i] for i in range(len(input_data))}
        chunk_size = len(input_data) // self.M
        idx_chunks = {i // self.M : list(range(i, i + chunk_size)) for i in range(0, len(input_data), chunk_size)}
        return chunks, idx_chunks


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
        data = self.socket.recv_json()

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
            self.socket.recv_string()

        # stage 4 onwards: Mappers do nothing
        self.clear_socket()

    def process_data(self, data):
        out = []
        for k, v in data.items():
            for chunk in v.split():
                out.append(self.map_f(k, chunk))
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
            msg = self.socket.recv_string()
            msgs.append(msg)
            self.socket.send_string(str(time.time()))

        print(f"R{self.my_id}: received all messages from mappers: {msgs}")

        self.clear_socket()

        # stage 4: Reducer processes data (B:_ M:_ R:_)
        parsed = self.parse(msgs)
        output = self.process_data(parsed)

        # stage 5: Reducer writes processed date to output file
        self.write_output(output)

        # step 6: Reducer tells Master it is done (B:S M:_ R:L)
        self.socket = self.create_socket("REQ")
        self.connect(self.master_id)
        self.socket.send_string(f"done (R:{self.my_id})")
        self.socket.recv_string()

        # stage 7: Reducer does nothing
        self.clear_socket()

    def process_data(self, data):
        result = {k: 0 for k, v in data}
        for k, v in data:
            result[k] = self.reduce_f(result[k], v)

        # to string
        items = []
        for k, v in result.items():
            items.append(f"{k}:{v}")

        return " ".join(items)

    def write_output(self, output):
        loc = self.tmp_dir / f"{self.my_id}.txt"
        with open(loc, "w") as f:
            f.write(output)

    def parse(self, data):
        stripped_msgs = [msg[1:-1] for msg in data]
        joined_msgs = " ".join(stripped_msgs)
        return [tuple(kv.split(":")) for kv in joined_msgs.split()]
