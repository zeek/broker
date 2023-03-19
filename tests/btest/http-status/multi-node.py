# Like `single-node` but peers with another node and attaches master and clone
# stores.
#
# Note: waiting for '"master-id": "36762b90-d415-4ada-bb6a-eff33f34836b"' makes
# sure that the clone has found its master store.
#
# @TEST-GROUP: http-status
#
# @TEST-PORT: PEERING_PORT
# @TEST-PORT: HTTP_PORT
#
# @TEST-EXEC: btest-bg-run node1 "broker-node --config-file=../node1.cfg -p $PEERING_PORT --broker.metrics.port=$HTTP_PORT"
# @TEST-EXEC: btest-bg-run node2 "broker-node --config-file=../node2.cfg --peers='<tcp://localhost:$PEERING_PORT>'"
# @TEST-EXEC: $SCRIPTS/wait-for-http $HTTP_PORT v1/status/json 30 '@awaited-keys.txt' || (btest-bg-wait -k 1 && false)
# @TEST-EXEC: $SCRIPTS/read-http $HTTP_PORT v1/status/json response.json
# @TEST-EXEC: python3 $SCRIPTS/extract-json-keys.py response.json > keys.json
# @TEST-EXEC: btest-diff keys.json

# Configure our broker with verbose output in order to have some artifacts to
# look at when things go wrong.
@TEST-START-FILE node1.cfg

caf {
  logger {
    file {
      path = 'broker.log'
      verbosity = 'debug'

    }
  }
}
master-stores = ["my-store-1", "my-store-2"]
clone-stores = ["my-store-3"]
topics = ["/test"]
verbose = true
endpoint-id = "d207546e-7780-48f4-bfdf-f47f3c789dc6"

@TEST-END-FILE

@TEST-START-FILE node2.cfg

caf {
  logger {
    file {
      path = 'broker.log'
      verbosity = 'debug'
    }
  }
}
master-stores = ["my-store-3"]
clone-stores = ["my-store-1", "my-store-2"]
topics = ["/test"]
verbose = true
endpoint-id = "36762b90-d415-4ada-bb6a-eff33f34836b"

@TEST-END-FILE

# A list of keys that must appear after the nodes have connected their stores.
@TEST-START-FILE awaited-keys.txt
clones.my-store-3.master-id
masters.my-store-1.inputs.36762b90-d415-4ada-bb6a-eff33f34836b
masters.my-store-2.inputs.36762b90-d415-4ada-bb6a-eff33f34836b
@TEST-END-FILE
