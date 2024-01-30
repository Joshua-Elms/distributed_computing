from core import Client
import time

c1 = Client(
    HOST = "127.0.0.1",
    PORT = 65000,
)

response = c1.set(b"key1", b"C")
print(f"CLIENT1: Received {response} to set")

response = c1.set(b"key2", b"B")
print(f"CLIENT1: Received {response} to set")

response = c1.set(b"key2", b"A")

msg, value, end = c1.get(b"key2")
print(f"CLIENT1: Received\n{msg}\n{value}\n{end} to get")

c1.close()