from core import *
import time
import subprocess
import json

def main(test_num, server_info):
    """
    Run tests
    """
    print(f"Beginning test {test_num}")
    HOST = server_info["HOST"]
    PORT = int(server_info["PORT"])
    set = lambda key, value: f"set {key} {value}"
    get = lambda key: f"get {key}"

    match test_num:
        case 1: 
            # open server
            subprocess.Popen(["python", "popen_server.py", json.dumps(server_info)])

            # prepare client instructions
            base_client_info = dict(HOST=HOST, PORT=PORT)
            client_info = {
                **base_client_info,
                "id": 1,
                "instructions": (
                    set("name", "John Doe"),
                    get("name"),
                    set("age", 25),
                    get("age"),
                    set("name", "Jane Doe"),
                    get("name")
                )
            }
            # open client
            subprocess.Popen(["python", "popen_client.py", json.dumps(client_info)])
    
    
if __name__ == "__main__":
    main(
        test_num=1,
        server_info=dict(
            HOST="127.0.0.1",
            PORT=65000,
            timeout=120,
            backlog=10,
            kvstore_path="kvstore.data"
        )
    )