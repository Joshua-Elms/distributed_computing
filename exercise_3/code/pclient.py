#
#   Hello World client in Python
#   Connects REQ socket to tcp://localhost:5555
#   Sends "Hello" to server, expects "World" back
#

import zmq
import time

context = zmq.Context()

#  Socket to talk to server
print("Connecting to hello world server…")
socket = context.socket(zmq.REQ)
socket.connect("tcp://127.0.0.1:9999")

#  Do 10 requests, waiting each time for a response
for request in range(3):
    print(f"Sending request {request} …")
    socket.send(b"Hello")

    #  Get the reply.
    message = socket.recv()
    print(f"Received reply {message}")

time.sleep(1)
socket.send(b"exit")
socket.disconnect("tcp://127.0.0.1:9999")
socket.bind("tcp://*:9808")
while True:
    msg = socket.recv()
    print(f"Received: {msg}")
    
    time.sleep(1)

    socket.send(b"Final communique")
