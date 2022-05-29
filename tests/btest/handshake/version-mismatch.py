# @TEST-GROUP: native
#
# @TEST-PORT: BROKER_PORT
#
# @TEST-COPY-FILE: ${SCRIPTS}/wire_format.py
#
# @TEST-EXEC: btest-bg-run node "broker-node --config-file=../node.cfg"
# @TEST-EXEC: btest-bg-run peer "python3 ../peer.py >peer.out"
#
# @TEST-EXEC: $SCRIPTS/wait-for-file peer/done 30 || (btest-bg-wait -k 1 && false)
# @TEST-EXEC: btest-diff peer/peer.out
# @TEST-EXEC: btest-bg-wait -k 1

@TEST-START-FILE node.cfg

broker {
  disable-ssl = true
}
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

@TEST-START-FILE peer.py

import os, sys, uuid

sys.path.insert(0, '..')

from wire_format import *

port = int(os.environ['BROKER_PORT'].split('/')[0])

# The biggest possible version-4 UUID to make sure we are the responder.
this_peer = uuid.UUID('ffffffff-ffff-4fff-bfff-ffffffffffff')

# The (failed) handshake sequence for a version mismatch is:
# <- probe
# -> hello
# <- drop_conn

def run(fd):
    # <- probe
    (tag, msg) = read_hs_msg(fd)
    check_eq(tag, MessageType.PROBE)
    check_eq(msg.magic, magic_number())
    # -> hello
    write_hs_msg(fd, MessageType.HELLO, pack_hello(this_peer, 200, 201))
    # <- drop_conn
    (tag, msg) = read_hs_msg(fd)
    check_eq(tag, MessageType.DROP_CONN)
    check_eq(msg.code, 2) # 2 = ec::peer_incompatible


test_main("localhost", port, run)

@TEST-END-FILE
