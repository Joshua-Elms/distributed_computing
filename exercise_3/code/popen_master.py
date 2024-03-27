import zmq
import json
import sys
import time
from core import *

config = json.loads(sys.argv[1])
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:9999")

while True:
    #  Wait for next request from client
    message = socket.recv()
    print(f"Received request: {message}")

    #  Do some 'work'
    time.sleep(config['sleep'])

    #  Send reply back to client
    socket.send(config['response'].encode())