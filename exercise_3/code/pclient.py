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
C = context.socket(zmq.REQ)
C.connect("tcp://127.0.0.1:9999")

#  Do 10 requests, waiting each time for a response
for request in range(3):
    print(f"Sending request {request} …")
    C.send(b"Hello")

    #  Get the reply.
    message = C.recv()
    print(f"Received reply {message}")

time.sleep(1)
C.send(b"exit")
S = context.socket(zmq.REP)
# S.disconnect("tcp://127.0.0.1:9999")
S.bind("tcp://*:9808")
msg = S.recv()
print(f"Received: {msg}")
    
time.sleep(1)

S.send(b"Final communique")


