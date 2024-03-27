#   Hello World server in Python
#   Binds REP socket to tcp://*:5555
#   Expects b"Hello" from client, replies with b"World"

import zmq
import json
import sys
import time

context = zmq.Context()
S = context.socket(zmq.REP)
C = context.socket(zmq.REQ)
S.bind("tcp://*:9999")

while True:
    #  Wait for next request from client
    message = S.recv()
    if message == b"exit":
        print("Received exit")
        break
    print("Received request: %s" % message)

    #  Do some 'work'
    time.sleep(1)

    #  Send reply back to client
    S.send(b"World")

breakpoint()

time.sleep(3)
C.connect("tcp://127.0.0.1:9808")
C.send(b"Holla back")
msg = C.recv()
print(f"Received: {msg}")
