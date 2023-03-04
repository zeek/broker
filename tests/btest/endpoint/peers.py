# @TEST-GROUP: endpoint
#
# @TEST-PORT: BROKER_PORT
#
# @TEST-EXEC: btest-bg-run node1 "broker-node --config-file=../node.cfg"
# @TEST-EXEC: python3 mk-test-cfg.py
# @TEST-EXEC: btest-peers --config-file=test.cfg > out.txt
# @TEST-EXEC: btest-bg-wait -k 1
# @TEST-EXEC: btest-diff out.txt

# Turn on debug logging on our test programs.
@TEST-START-FILE node.cfg

caf {
  logger {
    file {
      path = 'broker.log'
      verbosity = 'debug'
    }
  }
}
topics = ["/test"]
verbose = true

@TEST-END-FILE

# Helper script for generating the config for btest-peers.
@TEST-START-FILE mk-test-cfg.py

import json
import os

port = os.environ['BROKER_PORT'].split('/')[0]

cfg = {
    "caf": {
        "logger": {
            "file": {
                "path": "broker.log",
                "verbosity": 'debug',
            },
        },
    },
    "peers": [
        f"tcp://localhost:{port}",
    ],
}

with open('test.cfg', 'w') as f:
    json.dump(cfg, f)

@TEST-END-FILE
