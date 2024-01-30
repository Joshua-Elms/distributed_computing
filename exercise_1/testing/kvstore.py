import socket
import time
import pprint
from pathlib import Path

END = br"\r\n"
BUF_SIZE = 4096
SLEEPTIME = 0.05
INT_SIZE = 4

class KVStore:
    def __init__(self, path):
        self.store = {}
        self.path = Path(path)

    def get(self, key):
        try:
            value = self.store[key]
            size = len(value)
            return (self.store[key], size)
        
        except KeyError as e:
            return (b"KEY NOT FOUND " + END, None)

    def set(self, key, value):
        try:
            self.store[key] = value
            status = b"STORED " + END
        except KeyError as e:
            status = b"NOT STORED " + END

        return status

    def __str__(self):
        return str(self.store)

    def display(self):
        pprint.pprint(self.store, depth=2)

def main(path: str):
    kvs = KVStore(path)
    k1 = b"key1"
    s1 = kvs.set(k1, b"A")
    v1, l1 = kvs.get(b"key1")

    print(f"Key: {k1}\nValue: {v1}\n")

if __name__=="__main__":
    main(path="exercise_1/testing/kvstore.data")
