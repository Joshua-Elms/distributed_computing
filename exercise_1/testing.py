from core import *
import time
import subprocess
import json
from pathlib import Path

def main(test_num, server_info):
    """
    Run tests
    """
    print(f"Beginning test {test_num}")
    HOST = server_info["HOST"]
    PORT = int(server_info["PORT"])
    set = lambda key, value: f"set {key} {value}"
    get = lambda key: f"get {key}"
    procs = []

    match test_num:
        case 1: 
            # open server
            server_proc = subprocess.Popen(["python", "popen_server.py", json.dumps(server_info)], close_fds=True)
            procs.append(server_proc)

            time.sleep(1)

            # prepare client instructions
            base_client_info = dict(HOST=HOST, PORT=PORT, vocal=True)
            # open client
            for i in range(1, 4):
                time.sleep(0.1)
                client_info = {
                    **base_client_info,
                    "id": i,
                    "instructions": (
                        set("name", "John Doe"),
                        get("name"),
                        set("age", i*5),
                        get("age"),
                        set("name", "Jane Doe"),
                        get("name")
                    )
                }
                client_proc = subprocess.Popen(["python", "popen_client.py", json.dumps(client_info)])
                procs.append(client_proc)

        case 2: 
            # open server
            server_proc = subprocess.Popen(["python", "popen_server.py", json.dumps(server_info)], close_fds=True)
            procs.append(server_proc)

            time.sleep(1)

            # prepare client instructions
            base_client_info = dict(HOST=HOST, PORT=PORT, vocal=True)
            client_info = {**base_client_info, "id": 1, "instructions": []}
            collatz = lambda x, n=0: n if x == 1 else collatz(x * 3 + 1, n+1) if x % 2 else collatz (x // 2, n + 1)
            # open client
            for i in range(80, 100):
                instr = set(f"len(collatz({i}))", collatz(i))
                client_info["instructions"].append(instr)

            for i in range(90, 102):
                instr = get(f"len(collatz({i}))")
                client_info["instructions"].append(instr)

            client_proc = subprocess.Popen(["python", "popen_client.py", json.dumps(client_info)])
            procs.append(client_proc)

        case 3: 
            # open server
            server_proc = subprocess.Popen(["python", "popen_server.py", json.dumps(server_info)], close_fds=True)
            procs.append(server_proc)

            time.sleep(1)

            # prepare bad client instructions (key too long)
            base_client_info = dict(HOST=HOST, PORT=PORT, vocal=True)
            client_info = {
                **base_client_info, 
                "id": 1, 
                "instructions": [
                    set("a"*60, "key not too long"),
                    get("a"*60),
                    set("a"*251, "key too long"),
                    get("a"*251)
                ]
                }

            client_proc = subprocess.Popen(["python", "popen_client.py", json.dumps(client_info)])
            procs.append(client_proc)

        case 4: 
            # open server
            server_proc = subprocess.Popen(["python", "popen_server.py", json.dumps(server_info)], close_fds=True)
            procs.append(server_proc)

            time.sleep(1)

            # prepare bad client instructions (malformed)
            base_client_info = dict(HOST=HOST, PORT=PORT, vocal=True)
            client_info = {
                **base_client_info, 
                "id": 1, 
                "instructions": [
                    "get key", # key doesn't exist yet
                    "set keyvalue key value", # if command is space delimited, this would be problem. (Client should be able to handle this)
                ]
                }

            client_proc = subprocess.Popen(["python", "popen_client.py", json.dumps(client_info)])
            procs.append(client_proc)

    # fix so that server and client processes are killed, not useful for actual memcache server
    while True:
        time.sleep(1)
        all_done = True
        for proc in procs:
            if proc.poll() is None:
                all_done = False
                break
        if all_done:
            break

    for proc in procs:
        proc.kill()
    
    
if __name__ == "__main__":
    main(
        test_num=4,
        server_info=dict(
            HOST="127.0.0.1",
            PORT=65000,
            timeout=3,
            backlog=10,
            kvstore_path="kvstore.data"
        )
    )