from core import Client
import json
import sys

client_info = json.loads(sys.argv[1])
HOST = client_info["HOST"]
PORT = int(client_info["PORT"])
id = client_info["id"]
instructions = client_info["instructions"]
print(f"CLIENT{id}: Received instructions: {instructions}")

client = Client(
    HOST=HOST,
    PORT=PORT,
    connection_timeout=60,
)

# execute instructions

# for some reason, python doesn't have an lsplit function
lsplit_bytes = lambda s, c, n: [bytes(x[::-1]) for x in s[::-1].rsplit(c, n)[::-1]]

for i, instruction in enumerate(instructions):
    print(f"CLIENT{id}: Executing instruction {i}: {instruction}")
    if instruction.startswith(b"get"):
        parts = lsplit_bytes(instruction, " ", 1)
        response = client.get(key=parts[1])

    elif instruction.startswith(b"set"):
        parts = lsplit_bytes(instruction, " ", 2)
        response = client.set(key=parts[1], value=parts[2])

    else: 
        print(f"CLIENT{id}: Invalid instruction: {instruction}")
    
    print(f"CLIENT{id}: Received response: {response}")

# close connection
client.close()
print(f"CLIENT{id}: Connection closed")