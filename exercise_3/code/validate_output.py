from pathlib import Path

def read_and_parse(path):
    with open(path, "r") as f:
        raw = f.read()

    data_str = raw.split()
    data = sorted([tuple(item.split(":")) for item in data_str])

    return data

def compare(parallel, serial, task):
    parallel_data = read_and_parse(parallel)
    serial_data = read_and_parse(serial)

    if parallel_data == serial_data:
        print(f"MapReduce ({task}) passed")
    else:
        print(f"MapReduce ({task}) failed")

if __name__=="__main__":
    compare(Path("output/parallel_WC.txt"), Path("output/serial_WC.txt"), "Word Count")
    compare(Path("output/parallel_II.txt"), Path("output/serial_II.txt"), "Inverted Index")