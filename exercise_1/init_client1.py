from core import Client
import time

c1 = Client(
    HOST = "127.0.0.1",
    PORT = 65000,
)



print(f"CLIENT: I am about to transmit a SET message to server at t={time.time()}")
response = c1.set(b"key1", b"value1")
print(f"CLIENT: Received {response.decode('utf-8')} at t={time.time()}")
print(f"CLIENT: I am about to transmit a GET message to server at t={time.time()}")
msg, value, end = c1.get("key1")
print(repr(f"CLIENT: GET message: \n{msg.decode('utf-8')} {value.decode('utf-8')} {end.decode('utf-8')} at t={time.time()}"))
c1.close()