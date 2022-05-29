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

import os, sys, socket, uuid, time

sys.path.insert(0, '..')

from wire_format import *

port = int(os.environ['BROKER_PORT'].split('/')[0])

# The smallest possible version-4 UUID to make sure we are the originator.
this_peer = uuid.UUID('00000000-0000-4000-8000-000000000000')

# The handshake sequence is:
# <- probe
# -> hello
# <- hello (since we have a smaller ID)
# -> version_select
# -> originator_syn_msg
# <- responder_syn_ack
# -> originator_ack
#
# After that, the peering is established and we do one round of ping-pong:
# -> ping
# <- pong (same payload)

def run(fd):
    # <- probe
    (tag, msg) = read_hs_msg(fd)
    check_eq(tag, MessageType.PROBE)
    check_eq(msg.magic, magic_number())
    # -> hello
    write_hs_msg(fd, MessageType.HELLO, pack_hello(this_peer, 1, 99))
    # <- hello
    (tag, msg) = read_hs_msg(fd)
    check_eq(tag, MessageType.HELLO)
    check_eq(msg.magic, magic_number())
    check_eq(msg.min_version, 1)
    check_eq(msg.max_version, 1)
    peer_id = msg.sender_id
    # -> version_select
    write_hs_msg(fd, MessageType.VERSION_SELECT, pack_version_select(this_peer, 1))
    # -> originator_syn_msg
    write_hs_msg(fd, MessageType.ORIGINATOR_SYN, pack_originator_syn(['/foo/bar']))
    # <- responder_syn_ack
    (tag, msg) = read_hs_msg(fd)
    check_eq(tag, MessageType.RESPONDER_SYN_ACK)
    check_eq(msg.subscriptions, ['/test'])
    # -> originator_ack
    write_hs_msg(fd, MessageType.ORIGINATOR_ACK, pack_originator_ack())
    # -> ping("foo")
    write_op_msg(fd, this_peer, peer_id, MessageType.PING, '<$>',
                 pack_ping("foo".encode()))
    # <- pong("foo")
    (tag, msg) = read_op_msg(fd)
    check_eq(tag, MessageType.PONG)
    check_eq(msg.sender_id, peer_id)
    check_eq(msg.receiver_id, this_peer)
    check_eq(msg.topic, '<$>')
    check_eq(msg.payload.decode(), 'foo')


test_main("localhost", port, run)

@TEST-END-FILE
