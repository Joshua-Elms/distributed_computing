import socket
import time
import pprint
from pathlib import Path

END = br"\r\n"
BUF_SIZE = 4096
SLEEPTIME = 0.05
INT_SIZE = 4
KEY_SIZE = 60

class KVStore:
    def __init__(self, path):
        self.store = {}
        self.path = Path(path)

    def get(self, key):
        value, size, pos = None, None, None
        with self.path.open("rb") as f:
            for i, line in enumerate(f):
                if line.startswith(key):
                    pos = i
                    key = line[:KEY_SIZE]
                    size = int.from_bytes(line[KEY_SIZE:KEY_SIZE+INT_SIZE], byteorder="big")
                    value = line[KEY_SIZE+INT_SIZE:KEY_SIZE+INT_SIZE+size]
                    break

        return dict(key=key, value=value, size=size, pos=pos)
        

    def set(self, key: bytes, value: bytes, size = None):
        size = len(value).to_bytes(INT_SIZE, byteorder="big") # will be replaced by actual size once this code goes into core.py
        key_exists = self.get(key) != (b"KEY NOT FOUND " + END, None) # this is the message returned if the key is not found
        key_exists = False # for testing purposes

        try:
            if key_exists: # trigger rewrite, key will be overwritten at bottom of file
                pass # rewrite

            else: # simply append key-size-value to bottom of file
                with self.path.open("ab") as f:
                    line = key + size + value + b"\n"
                    f.write(line)

                status = b"STORED " + END

        except Exception as e:
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
    print(f"Status: {s1}")
    # v1, l1 = kvs.get(b"key1")

    # print(f"Key: {k1}\nValue: {v1}\n")

if __name__=="__main__":
    main(path="kvstore.data")
