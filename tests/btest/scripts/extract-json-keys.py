# This script has two modes.
#
# extract-json-keys.py [<json file>]:
#   Opens <json file> and then extracts, sorts and prints all keys. When
#   omitting <json file> or passing '-', the script reads from STDIN.
#
# extract-json-keys.py <json file> <key list>:
#   Opens <json file> and checks whether all keys from <key list> appear,
#   whereas <key list> is either a comma-separated list or a file path prefixed
#   with an '@'.

import sys, json

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

def do_scan(fname):
    if fname == '-':
        rec_scan_keys(json.load(sys.stdin), None)
        return
    with open(fname) as f:
        rec_scan_keys(json.load(f), None)

do_scan('-' if len(sys.argv) < 2  else sys.argv[1])

# Mode 2? Check all keys.
if len(sys.argv) == 3:
    # Get the keys we are looking for.
    required_keys = []
    ls = sys.argv[2]
    if ls.startswith('@'):
        with open(ls[1:]) as f:
            required_keys = [line.rstrip() for line in f]
    else:
        required_keys.split(',')
    # Check that each required key exists in the input.
    print("required_keys: {}".format(required_keys))
    for key in required_keys:
        if not key in keys:
            sys.exit(1)
    sys.exit(0)

# Mode 1: just print the keys.
keys.sort()
for key in keys:
    print(key)
