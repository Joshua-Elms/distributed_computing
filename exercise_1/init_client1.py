from core import Client
import time

c1 = Client(
    HOST = "127.0.0.1",
    PORT = 65000,
)

response = c1.set(b"key1", b"value1")
print(f"CLIENT: Received {response} to set")

response = c1.set(b"key2", b"value3")
print(f"CLIENT: Received {response} to set")

response = c1.set(b"key2", b"yohofiddledy dee, a pirates life for me")

msg, value, end = c1.get(b"key2")
print(f"CLIENT: Received\n{msg}\n{value}\n{end}")

c1.close()