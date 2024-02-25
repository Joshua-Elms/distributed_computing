from core import *
import json
import sys

config_dict = json.loads(sys.argv[1])
config_idx = config_dict["config_idx"]
config_path = config_dict["config_path"]

process = Application(config_idx, config_path)