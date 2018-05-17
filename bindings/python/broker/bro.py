
try:
    from . import _broker
except ImportError:
    import _broker

import broker

class Event(_broker.bro.Event):
    def __init__(self, *args):
        if len(args) == 1 and isinstance(args[0], list):
            # Parse raw broker message as event.
            _broker.bro.Event.__init__(self, broker.Data.from_py(args[0]))
        elif len(args) == 2 and isinstance(args[0], str) and not isinstance(args[0], broker.Data):
            # (name, (arg1, arg2, ...))
            _broker.bro.Event.__init__(self, args[0], broker.Data.from_py(args[1]))
        else:
            # (name, arg1, arg2, ...)
            _broker.bro.Event.__init__(self, args[0], broker.Data.from_py(args[1:]))

    def args(self):
        return [broker.Data.to_py(a) for a in _broker.bro.Event.args(self)]
