import struct, uuid, sys, socket, time

from collections import namedtuple
from enum import IntEnum
from inspect import getframeinfo, stack


class MessageType(IntEnum):
  DATA = 1
  COMMAND = 2
  ROUTING_UPDATE = 3
  PING = 4
  PONG = 5
  HELLO = 6
  PROBE = 7
  VERSION_SELECT = 8
  DROP_CONN = 9
  ORIGINATOR_SYN = 10
  RESPONDER_SYN_ACK = 11
  ORIGINATOR_ACK = 12


# Returns Broker's magic number.
def magic_number():
    return 0x5A45454B


# Splits a buffer into head and tail.
def behead(buf, size):
    return (buf[:size], buf[size:])


# Returns its argument as varbyte-encoded buffer.
def vb_encode(n):
    buf = bytearray()
    x = n
    while x > 0x7f:
        buf.append((x & 0x7f) | 0x80)
        x = x >> 7
    buf.append(x & 0x7f)
    return buf


# Decodes a number from a varbyte-encoded buffer.
def vb_decode(buf):
    x = 0
    n = 0
    low7 = 0
    while True:
        low7 = buf[0]
        buf = buf[1:]
        x = x | ((low7 & 0x7f) << (7 * n))
        n += 1
        if low7 & 0x80 == 0:
            return (x, buf)


# Packs a Broker subscription into a buffer.
def pack_subscriptions(subs):
    buf = bytearray(vb_encode(len(subs)))
    for sub in subs:
        buf.extend(vb_encode(len(sub)))
        buf.extend(sub.encode())
    return buf


# Packs a string to a buffer. The resulting buffer contains the size as
# varbyte-encoded prefix followed by the characters.
def pack_string(x):
    buf = vb_encode(len(x))
    buf.extend(x.encode())
    return buf


# Unpacks a string from a buffer, the buffer must contain the size as
# varbyte-encoded prefix followed by the characters.
def unpack_string(buf):
    (str_size, tail) = vb_decode(buf)
    (head, remainder) = behead(tail, str_size)
    return (head.decode(), remainder)


# Unpacks a Broker subscription.
def unpack_subscriptions(buf):
    result = []
    (size, remainder) = vb_decode(buf)
    print(f'unpack subscription of size {size}')
    for i in range(size):
        (sub, remainder) = unpack_string(remainder)
        result.append(sub)
    return (result, remainder)


# -- pack and unpack functions for handshake messages --------------------------

def unpack_hello(buf):
    HelloMsg = namedtuple('HelloMsg',
                          ['magic', 'sender_id', 'min_version', 'max_version'])
    (magic, uuid_bytes, vmin, vmax) = struct.unpack('!I16sBB', buf)
    return HelloMsg(magic, uuid.UUID(bytes=uuid_bytes), vmin, vmax)


def pack_hello(sender_id, vmin, vmax):
    return struct.pack('!I16sBB', magic_number(), sender_id.bytes, vmin, vmax)


def unpack_probe(buf):
    ProbeMsg = namedtuple('ProbeMsg', ['magic'])
    magic = struct.unpack('!I', buf)[0]
    return ProbeMsg(magic)


def pack_probe(msg):
    (magic) = msg
    return struct.pack('!I', magic)


def pack_version_select(sender_id, version):
    return struct.pack('!I16sB', magic_number(), sender_id.bytes, version)


def unpack_version_select(buf):
    VersionSelectMsg = namedtuple('VersionSelectMsg',
                                  ['magic', 'sender_id', 'version'])
    (magic, sender_id, version) = struct.unpack('!I16sB', buf)
    return VersionSelectMsg(magic, uuid.UUID(bytes=sender_id), version)


def pack_drop_conn(sender_id, code, description):
    buf = struct.pack('!I16sB', magic_number(), sender_id.bytes, code)
    arr = bytearray(buf)
    arr.extend(pack_string(description))
    return arr


def unpack_drop_conn(buf):
    DropConnMsg = namedtuple('DropConnMsg',
                             ['magic', 'sender_id', 'code', 'description'])
    (head, tail) = behead(buf, 21)
    (magic, sender_id, code) = struct.unpack('!I16sB', head)
    description = unpack_string(tail)
    return DropConnMsg(magic, uuid.UUID(bytes=sender_id), code, description)


def pack_originator_ack():
    return bytearray()


def unpack_originator_ack(buf):
    return ()


def pack_originator_syn(subs):
    return pack_subscriptions(subs)


def unpack_originator_syn(buf):
    (subscriptions, remainder) = unpack_subscriptions(buf)
    if len(remainder) > 0:
        raise RuntimeError('unpack_originator_syn: trailing bytes')
    OriginatorSynMsg = namedtuple('OriginatorSynMsg', ['subscriptions'])
    return OriginatorSynMsg(subscriptions)


def pack_responder_syn_ack(subs):
    return pack_subscriptions(subs)


def unpack_responder_syn_ack(buf):
    (subscriptions, remainder) = unpack_subscriptions(buf)
    if len(remainder) > 0:
        raise RuntimeError('unpack_responder_syn_ack: trailing bytes')
    ResponderSynAck = namedtuple('ResponderSynAck', ['subscriptions'])
    return ResponderSynAck(subscriptions)


# -- utility functions for socket I/O on handshake messages --------------------

def enum_to_str(e):
    # For compatibility around Python 3.11, this dictates how to render an enum
    # to a string. This changed in 3.11 for some enum types.
    return type(e).__name__ + '.' + e.name

# Reads a handshake message (phase 1 and phase 2).
def read_hs_msg(fd):
    # -1 since we extract the tag right away
    msg_len = int.from_bytes(fd.recv(4), byteorder='big', signed=False) - 1
    tag = MessageType(fd.recv(1)[0])
    tag_str = enum_to_str(tag)
    print(f'received a {tag_str} message with {msg_len} bytes')
    unpack_tbl = {
        MessageType.HELLO: unpack_hello,
        MessageType.PROBE: unpack_probe,
        MessageType.VERSION_SELECT: unpack_version_select,
        MessageType.DROP_CONN: unpack_drop_conn,
        MessageType.ORIGINATOR_SYN: unpack_originator_syn,
        MessageType.RESPONDER_SYN_ACK: unpack_responder_syn_ack,
        MessageType.ORIGINATOR_ACK: unpack_originator_ack,
    }
    return (tag, unpack_tbl[tag](fd.recv(msg_len)))


# Writes a handshake message (phase 1 and phase 2).
def write_hs_msg(fd, tag, buf):
    payload_len = len(buf) + 1
    fd.send(payload_len.to_bytes(4, byteorder='big', signed=False))
    fd.send(int(tag).to_bytes(1, byteorder='big', signed=False))
    fd.send(buf)
    tag_str = enum_to_str(tag)
    print(f'sent {tag_str} message with {payload_len} bytes')


# -- pack and unpack functions for phase 3 messages ----------------------------

def pack_ping(buf):
    return buf


def unpack_ping(buf):
    return buf


def pack_pong(buf):
    return buf


def unpack_pong(buf):
    return buf


# -- utility functions for socket I/O on phase 3 messages ----------------------

# Reads an operation-mode message (phase 3).
def read_op_msg(fd):
    # -35 since we extract the two IDs plus tag, TTL and topic len right away
    msg_len = int.from_bytes(fd.recv(4), byteorder='big', signed=False) - 37
    src = uuid.UUID(bytes=struct.unpack('!16s', fd.recv(16))[0])
    dst = uuid.UUID(bytes=struct.unpack('!16s', fd.recv(16))[0])
    tag = MessageType(fd.recv(1)[0])
    ttl = int.from_bytes(fd.recv(2), byteorder='big', signed=False)
    topic_len = int.from_bytes(fd.recv(2), byteorder='big', signed=False)
    topic = fd.recv(topic_len).decode()
    buf = fd.recv(msg_len - topic_len)
    tag_str = enum_to_str(tag)
    print(f'received a {tag_str} with a payload of {msg_len} bytes')
    unpack_tbl = {
        MessageType.PING: unpack_ping,
        MessageType.PONG: unpack_pong,
    }
    NodeMsg = namedtuple('NodeMsg',
                         ['sender_id', 'receiver_id', 'ttl', 'topic', 'payload'])
    return (tag, NodeMsg(src, dst, ttl, topic, unpack_tbl[tag](buf)))


def write_op_msg(fd, src, dst, tag, topic, buf):
    payload_len = len(buf) + 37 + len(topic)
    fd.send(payload_len.to_bytes(4, byteorder='big', signed=False))
    fd.send(src.bytes) # sender UUID
    fd.send(dst.bytes) # receiver UUID
    fd.send(int(tag).to_bytes(1, byteorder='big', signed=False)) # msg type
    fd.send(int(1).to_bytes(2, byteorder='big', signed=False)) # ttl
    fd.send(len(topic).to_bytes(2, byteorder='big', signed=False))
    fd.send(topic.encode())
    fd.send(buf)
    tag_str = enum_to_str(tag)
    print(f'sent {tag_str} message with a payload of {payload_len} bytes')


# -- minimal testing DSL -------------------------------------------------------

def check_eq(got, want):
    caller = getframeinfo(stack()[1][0])
    line = caller.lineno
    if got != want:
        raise RuntimeError(f'line {line}: check failed -> {got} != {want}')
    print(f'line {line}: check passed')


def write_done():
    with open('done', 'w') as f:
        f.write('done')


# Tries to connect up to 30 times before giving up.
def test_main(host, port, fn):
    connected  = False
    for i in range(30):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as fd:
                fd.connect(("localhost", port))
                connected = True
                fn(fd)
                write_done()
                sys.exit()
        except Exception as ex:
            if not connected:
                print(f'failed to connect to localhost:{port}, try again', file=sys.stderr)
                time.sleep(1)
            else:
                print(str(ex))
                print(traceback.format_exc())
                write_done()
                sys.exit()
