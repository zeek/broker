# High-level test that checks whether Broker responds with JSON output on its
# metrics port for the path /v1/status/json. The baseline checks against the
# keys in the dictionary. This test only has a single node without any stores
# attached.
#
# @TEST-GROUP: http-status
#
# @TEST-PORT: BROKER_PORT
# @TEST-PORT: BROKER_METRICS_PORT
#
# @TEST-EXEC: btest-bg-run node "broker-node --config-file=../node.cfg"
# @TEST-EXEC: $SCRIPTS/wait-for-http $BROKER_METRICS_PORT v1/status/json 30 || (btest-bg-wait -k 1 && false)
# @TEST-EXEC: $SCRIPTS/read-http $BROKER_METRICS_PORT v1/status/json response.json
# @TEST-EXEC: python3 extract-keys.py > keys.json
# @TEST-EXEC: btest-diff keys.json

# Configure our broker with verbose output in order to have some artifacts to
# look at when things go wrong.
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

# Extracts all keys from a JSON file, sorts and prints them.
@TEST-START-FILE extract-keys.py

import json

keys = []

def rec_scan_keys(xs, prefix):
    for key, val in xs.items():
        if not prefix:
            full_key = key
        else:
            full_key = '{}.{}'.format(prefix, key)
        keys.append(full_key)
        if type(val) is dict:
            rec_scan_keys(val, full_key)

with open('response.json') as f:
    data = json.load(f)
    rec_scan_keys(data, None)
    keys.sort()
    for key in keys:
        print(key)

@TEST-END-FILE
