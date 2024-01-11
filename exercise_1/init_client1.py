from core import Client

c1 = Client(
    HOST = "127.0.0.1",
    PORT = 65000,
)

response = c1.set("key1", "value1")
print(f"CLIENT: Received {response.decode('utf-8')}")
msg, value, end = c1.get("key1")
print(repr(f"CLIENT: GET message: \n{msg.decode('utf-8')} {value.decode('utf-8')} {end.decode('utf-8')}"))
