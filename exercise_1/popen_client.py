from core import *
import json
import sys
from pathlib import Path

client_info = json.loads(sys.argv[1])
HOST = client_info["HOST"]
PORT = int(client_info["PORT"])
vocal = client_info["vocal"]
id = client_info["id"]
instructions = client_info["instructions"]

client = Client(
    HOST=HOST,
    PORT=PORT,
    connection_timeout=60,
    vocal=vocal
)

# execute instructions

# for some reason, python doesn't have an lsplit function
lsplit_bytes = lambda s, c, n: [x[::-1] for x in s[::-1].rsplit(c, n)[::-1]]

for i, instruction in enumerate(instructions):
    if instruction.startswith("get"):
        parts = lsplit_bytes(instruction, " ", 1)
        bparts = [p.encode("utf-8") for p in parts]
        status, response, _ = client.get(key=bparts[1])
        if status == b"KEY NOT FOUND " + END:
            response = "KEY NOT FOUND \r\n"
        print(f"CLIENT{id}: {i}th instruction {instruction!r} received response: {response!r}")

    elif instruction.startswith("set"):
        parts = lsplit_bytes(instruction, " ", 2)
        bparts = [p.encode("utf-8") for p in parts]
        response = client.set(key=bparts[1], value=bparts[2])
        print(f"CLIENT{id}: {i}th instruction {instruction!r} received response: {response!r}")

    else: 
        parts = lsplit_bytes(instruction, " ", 2)
        bparts = [p.encode("utf-8") for p in parts]
        response = client.set(key=bparts[1], value=bparts[2])    
        print(f"CLIENT{id}: {i}th instruction {instruction!r} received response: {response!r}")

# close connection
client.close()
print(f"CLIENT{id}: Connection closed")