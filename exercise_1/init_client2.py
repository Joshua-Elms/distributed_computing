from core import Client
import time

c1 = Client(
    HOST = "127.0.0.1",
    PORT = 65000,
)

response = c1.set(b"key_C2", b"A")
print(f"CLIENT2: Received {response} to set")

# time.sleep(3)

response = c1.set(b"key3", b"C")
print(f"CLIENT2: Received {response} to set")

# time.sleep(3)

response = c1.set(b"key2", b"B")

msg, value, end = c1.get(b"key2")
print(f"CLIENT2: Received\n{msg}\n{value}\n{end}")

c1.close()