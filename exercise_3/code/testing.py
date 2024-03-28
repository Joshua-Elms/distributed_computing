from core import *
import time
import subprocess
import json
import os
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
    my_env = os.environ.copy()
    my_env["PYTHONPATH"] = f"{os.getcwd()}/code/:{my_env['PYTHONPATH']}"
    my_env["PYTHONHASHSEED"] = "434"


    # start Master
    sconf = json.dumps({**config, "my_id": config["master_id"]})
    proc = subprocess.Popen(["python", "code/popen_master.py", sconf], env=my_env)
    procs.append(proc)

    # start Mappers
    for mid in config["mapper_ids"]:
        sconf = json.dumps({**config, "my_id": mid})
        proc = subprocess.Popen(["python", "code/popen_mapper.py", sconf], env=my_env)
        procs.append(proc)

    # start Reducers
    for rid in config["reducer_ids"]:
        sconf = json.dumps({**config, "my_id": rid})
        proc = subprocess.Popen(["python", "code/popen_reducer.py", sconf], env=my_env)
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
        Path("configs/dev_ii.json")
    )
