from core import *
import time
import subprocess
import json
from pathlib import Path


def main(test_num, config_info):
    """
    Run tests
    """
    print(f"Beginning test {test_num}")
    path = config_info["config_path"].resolve().as_posix()
    with open(path, "r") as f:
        config = json.load(f)
    procs = []

    match test_num:
        case 1:

            for idx, _ in config.items():
                proc = subprocess.Popen(
                    ["python", "popen_middleware.py", json.dumps({"config_idx": idx, "config_path": path})])
                procs.append(proc)

            for idx, _ in config.items():
                proc = subprocess.Popen(
                    ["python", "popen_app.py", json.dumps({"config_idx": idx, "config_path": path})])
                procs.append(proc)

    # fix so that server and client processes are killed, not useful for actual memcache server
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
        test_num=1,
        config_info=dict(
            config_path = Path("config_0.json"),
        )
    )
