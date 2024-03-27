import json
import sys
from core import *

config = json.loads(sys.argv[1])
mapper = Mapper(config)