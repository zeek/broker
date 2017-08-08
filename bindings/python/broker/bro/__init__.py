#
# Bro-specific shim on top of raw Broker messages.
#

from enum import Enum

import broker

_ProtocolVersion = broker.Count(1)
_TypeEvent = broker.Count(16826412281) # atom("event")

class Event(broker.Message):
    def __init__(self, *args):
        if len(args) == 1 and isinstance(args[0], list):
            # A message to parse.
            ((version, type_), data) = args[0]

            if version != _ProtocolVersion:
                raise ValueError("unknown Bro protocol version")

            if type_ != _TypeEvent:
                raise ValueError("not a Bro event")

            self.name = data[0]
            self.args = data[1:]

        else:
            if len(args) == 0:
                raise TypeError("wrong number of arguments to broker.bro.Event")

            self.name = args[0]
            self.args = args[1:]

    def to_broker(self):
        # Overridden from broker.Message
        return [[_ProtocolVersion, _TypeEvent], [self.name] + [a for a in self.args]]

    def __str__(self):
        return "{}({})".format(self.name, ", ".join([str(a) for a in self.args]))
