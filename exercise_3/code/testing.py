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
    sconf = json.dumps({**config, "my_id": config["master_id"]})
    proc = subprocess.Popen(["python", "popen_master.py", sconf])
    procs.append(proc)

    # start Mappers
    for mid in config["mapper_ids"]:
        sconf = json.dumps({**config, "my_id": mid})
        proc = subprocess.Popen(["python", "popen_mapper.py", sconf])
        procs.append(proc)

    # start Reducers
    for rid in config["reducer_ids"]:
        sconf = json.dumps({**config, "my_id": rid})
        proc = subprocess.Popen(["python", "popen_reducer.py", sconf])
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
        Path("../configs/dev.json")
    )
