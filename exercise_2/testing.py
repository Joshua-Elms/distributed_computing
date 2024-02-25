from core import *
import time
import subprocess
import json
from pathlib import Path


def main(config_path: Path):
    """
    Run tests
    """
    path = config_path.resolve().as_posix()
    with open(path, "r") as f:
        config = json.load(f)
    procs = []

    for idx, _ in config.items():
        proc = subprocess.Popen(
            ["python", "popen_middleware.py", json.dumps({"config_idx": idx, "config_path": path})])
        procs.append(proc)

        proc = subprocess.Popen(
            ["python", "popen_app.py", json.dumps({"config_idx": idx, "config_path": path})])
        procs.append(proc)

    # fix so that app and middleware processes are killed, not useful for actual memcache server
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
        Path("test_configs/config_2.json")
    )
