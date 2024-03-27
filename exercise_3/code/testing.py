from core import *
import time
import subprocess
import json
from pathlib import Path


def main(config_path: Path):
    """
    Run tests
    """
    # read config
    with open(config_path, "r") as f:
        config = json.load(f)

    # procs used to track processes
    procs = []

    ### run MapReduce ###

    # start Master
    proc = subprocess.Popen(["python", "popen_master.py", json.dumps(config["master"])])
    procs.append(proc)

    # start Mappers
    for k, v in config["mappers"].items():
        proc = subprocess.Popen(["python", "popen_mapper.py", json.dumps(v)])
        procs.append(proc)

    # start Reducers
    for k, v in config["reducers"].items():
        proc = subprocess.Popen(["python", "popen_reducer.py", json.dumps(v)])
        procs.append(proc)

    ### end MapReduce ###

    # fix so that MapReduce eventually terminates
    while True:
        time.sleep(1)
        all_done = True
        for proc in procs:
            if proc.poll() is None:
                all_done = False
                break
        if all_done:
            break

    for proc in procs:
        proc.kill()


if __name__ == "__main__":
    main(
        Path("configs/dev.json")
    )
