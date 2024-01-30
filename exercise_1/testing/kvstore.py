import socket
import time
import pprint
from pathlib import Path
import shutil

END = b"\r\n"
END_SIZE = len(END)
BUF_SIZE = 4096
SLEEPTIME = 0.05
INT_SIZE = 4
KEY_SIZE = 5
INT_ORDER = "big"

class KVStore:
    def __init__(self, path):
        self.store = {}
        self.path = Path(path)
        self.path.touch(exist_ok=True)
        self.tmp_path = self.path.with_suffix(".tmp")

    def get(self, key):
        value, size, pos = None, None, None
        with self.path.open("rb") as f:
            for i, line in enumerate(f):
                if line.startswith(key):
                    pos = i
                    key = line[:KEY_SIZE]
                    size = int.from_bytes(line[KEY_SIZE:KEY_SIZE+INT_SIZE], byteorder=INT_ORDER)
                    value = line[KEY_SIZE+INT_SIZE:KEY_SIZE+INT_SIZE+size]
                    break

        return dict(key=key, value=value, size=size, pos=pos)
        

    def set(self, key: bytes, value: bytes, size = None):
        size = len(value).to_bytes(INT_SIZE, byteorder=INT_ORDER) # will be replaced by actual size once this code goes into core.py
        key_response = self.get(key)
        key_exists = key_response["value"] is not None # used to determine whether to rewrite or append to file

        try:
            if key_exists: # trigger rewrite, key will be overwritten at bottom of file
                if key_response["value"] != value: # only rewrite if value is different
                    self._rewrite(key, value, size, key_response["pos"])

            else: # simply append key-size-value to bottom of file
                with self.path.open("a+b") as f:
                    line = key + size + value + b"\n"
                    f.write(line)

            status = b"STORED " + END

        except Exception as e:
            print(e)
            status = b"NOT STORED " + END

        return status
    
    def _rewrite(self, key, value, size, pos):
        shutil.copy(self.path, self.tmp_path) # copy to temp file so that we can overwrite lines from the original file
        with self.tmp_path.open("rb") as f_read:
            with self.path.open("r+b") as f_write:
                start = 0
                f_write.seek(0)
                for i, line in enumerate(f_read):
                    if i == pos:
                        f_write.seek(start, 0)

                    elif i > pos:
                        print(f"Writing line {line} at {f_write.tell()}")
                        f_write.write(line) # rewrite all lines after key

                    size_parsed = int.from_bytes(line[KEY_SIZE:KEY_SIZE+INT_SIZE], byteorder=INT_ORDER)
                    start += KEY_SIZE + INT_SIZE + size_parsed + 1 # Key + Size + Value + Newline (go to next line)

                f_write.write(key + size + value + b"\n") # overwrite key at bottom of file
                f_write.truncate()

        self.tmp_path.unlink()

    def __str__(self):
        return str(self.store)

    def display(self):
        pprint.pprint(self.store, depth=2)

def main(path: str):
    kvs = KVStore(path)
    for i in range(100):
        time.sleep(0.05)
        k = f"key{i}".encode('utf-8')
        if len(k) < KEY_SIZE:
            k += b" " * (KEY_SIZE - len(k))
        v = f"{chr(i + 65)}".encode('utf-8')
        s = kvs.set(k, v)
        print(f"Status: {s}")

    k1 = b"key3 "
    time.sleep(2)
    s1 = kvs.set(k1, b"TEST")
    print(f"Status: {s1}")
    k1 = b"key1 "
    time.sleep(2)
    s1 = kvs.set(k1, b"Oh Yeah")
    k1 = b"key2 "
    time.sleep(2)
    s1 = kvs.set(k1, b"I'm new and partial af")
    # kvs._rewrite(k1, b"B", 1, 2)
    # v1, l1 = kvs.get(b"key1")

    # print(f"Key: {k1}\nValue: {v1}\n")

if __name__=="__main__":
    main(path="kvstore.data")
