#   Hello World server in Python
#   Binds REP socket to tcp://*:5555
#   Expects b"Hello" from client, replies with b"World"

import time
import zmq

import zmq
import json
import sys
import time
from core import *

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:9999")

while True:
    #  Wait for next request from client
    message = socket.recv()
    if message == b"exit":
        print("Received exit")
        break
    print("Received request: %s" % message)

    #  Do some 'work'
    time.sleep(1)

    #  Send reply back to client
    socket.send(b"World")

time.sleep(1)
socket.connect("tcp://127.0.0.1:9808")
socket.send(b"Holla back")
msg = socket.recv()
print(f"Received: {msg}")
