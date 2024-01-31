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
        self.path = Path(path)
        self.path.touch(exist_ok=True)
        self.tmp_path = self.path.with_suffix(".tmp")

    def get(self, key):
        value, size, pos = None, None, None
        with self.path.open("rb") as f:
            value = None
            size = None
            start_pos = None
            end_pos = 0
            while True:
                current_key = f.read(KEY_SIZE)
                # if current_key is empty, we've reached the end of the file
                if not current_key:
                    break
                tmp_size = f.read(INT_SIZE)
                tmp_size_int = int.from_bytes(tmp_size, byteorder=INT_ORDER)
                end_pos += KEY_SIZE + INT_SIZE + tmp_size_int
                tmp_value = f.read(tmp_size_int)
                if current_key == key:
                    value = tmp_value
                    size = tmp_size
                    start_pos = end_pos - (KEY_SIZE + INT_SIZE + tmp_size_int)
                    break

        return dict(key=key, value=value, size=size, start_pos=start_pos, end_pos=end_pos)

    def set(self, key: bytes, value: bytes, size: bytes = None):
        """
        delete default value for size
        """
        # will be replaced by actual size once this code goes into core.py
        size = len(value).to_bytes(INT_SIZE, byteorder=INT_ORDER)
        key_response = self.get(key)
        # used to determine whether to rewrite or append to file
        key_exists = key_response["value"] is not None

        try:
            if key_exists:  # trigger rewrite, key will be overwritten at bottom of file
                if key_response["value"] != value:  # only rewrite if value is different
                    self._rewrite(key, value, size, key_response["start_pos"], key_response["end_pos"])

            else:  # simply append key-size-value to bottom of file
                with self.path.open("a+b") as f:
                    line = key + size + value
                    f.write(line)

            status = b"STORED " + END

        except Exception as e:
            print(e)
            status = b"NOT STORED " + END

        return status

    def _rewrite(self, key: bytes, value: bytes, size: bytes, start_pos: int, end_pos: int):
        # copy to temp file so that we can overwrite lines from the original file
        shutil.copy(self.path, self.tmp_path)
        with self.tmp_path.open("rb") as f_read:
            with self.path.open("r+b") as f_write:
                start = 0
                f_read.seek(end_pos)
                f_write.seek(start_pos)
                # print(f"pos: {pos}")
                f_write.truncate() # clear file from pos to end
                # this_key, this_size = f_read.read(KEY_SIZE), f_read.read(INT_SIZE)
                # this_size_int = int.from_bytes(this_size, byteorder=INT_ORDER)
                # f_read.seek(pos + KEY_SIZE + INT_SIZE + this_size_int) # go to end of key-value pair to be overwritten
                while True:
                    this_key = f_read.read(KEY_SIZE)
                    if not this_key:
                        break
                    this_size =  f_read.read(INT_SIZE)
                    this_size_int = int.from_bytes(this_size, byteorder=INT_ORDER)
                    this_value = f_read.read(this_size_int)
                    f_write.write(this_key + this_size + this_value)

                f_write.write(key + size + value)

                # for i, line in enumerate(f_read):
                #     if i == pos:
                #         f_write.seek(start, 0)

                #     elif i > pos:
                #         f_write.write(line)  # rewrite all lines after key

                #     size_parsed = int.from_bytes(
                #         line[KEY_SIZE:KEY_SIZE+INT_SIZE], byteorder=INT_ORDER)
                #     # Key + Size + Value + Newline (go to next line)
                #     start += KEY_SIZE + INT_SIZE + size_parsed + 1

                # # overwrite key at bottom of file
                # f_write.write(key + size + value + b"\n")
                # f_write.truncate()

        self.tmp_path.unlink()

    def __str__(self):
        output = f"Key-Value store at {self.path}\n{'-'*30}\n"

        lines = []
        max_int_digits = 0
        empty = True
        with self.path.open("rb") as f:
            while True:
                key = f.read(KEY_SIZE)
                if not key:
                    break
                size =  f.read(INT_SIZE)
                size_int = int.from_bytes(size, byteorder=INT_ORDER)
                int_digits = len(str(size_int))
                max_int_digits = int_digits if int_digits > max_int_digits else max_int_digits
                value = f.read(size_int)
                lines.append((key, size_int, value))
                empty = False

        for line in lines:
            output += f"{line[0]} {str(line[1]).rjust(max_int_digits)}b: {line[2]}\n"

        if empty:
                output += "Empty"

        output += "\n"

        return output

def main(path: str):
    kvs = KVStore(path)
    # for i in range(100):
    #     k = f"key{i}".encode('utf-8')
    #     if len(k) < KEY_SIZE:
    #         k += b" " * (KEY_SIZE - len(k))
    #     v = f"{chr(i + 65)}".encode('utf-8')
    #     s = kvs.set(k, v)
        # print(f"Status: {s}")

    k1 = b"key3 "
    time.sleep(2)
    s1 = kvs.set(k1, b"TEST")
    print(f"Status: {s1}")
    k1 = b"key1 "
    time.sleep(2)
    s1 = kvs.set(k1, b"Oh Yeah")
    print(f"Status: {s1}")
    k1 = b"key2 "
    time.sleep(2)
    s1 = kvs.set(k1, b"\n\n")
    print(f"Status: {s1}")
    k1 = b"key3 "
    time.sleep(2)
    s1 = kvs.set(k1, b"Rahhhhhhhh")
    print(f"Status: {s1}")
    # kvs._rewrite(k1, b"B", 1, 2)
    # d = kvs.get(b"key1")

    print(kvs)

    # print(f"get: {d}")

if __name__=="__main__":
    main(path="kvstore.data")
