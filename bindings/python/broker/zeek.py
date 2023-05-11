
try:
    from . import _broker
except ImportError:
    import _broker

import broker

# Keep this in sync with zeek.hh
class MetadataType:
    NetworkTimestamp = broker.Count(1)
    UserMetadataStart = broker.Count(200)

class Event(_broker.zeek.Event):
    def __init__(self, *args, metadata=None):
        if len(args) == 1 and not isinstance(args[0], str):
            # Parse raw broker message as event.
            _broker.zeek.Event.__init__(self, broker.Data.from_py(args[0]))
        else:
            # (name, arg1, arg2, ..., [metadata={...}])
            broker_metadata = None
            if metadata is not None:
                # Convert dicts to lists and make a copy so we don't
                # modify the callers argument.
                metadata = list(metadata.items() if hasattr(metadata, "items") else metadata)
                # Convert non-counts to counts for convencience.
                for i, m in enumerate(metadata):
                    if not isinstance(m[0], broker.Count):
                        metadata[i] = (broker.Count(m[0]), m[1])

                broker_metadata = broker.Data.from_py(metadata)

            _broker.zeek.Event.__init__(self, args[0], broker.Data.from_py(args[1:]),
                                        metadata=broker_metadata)

    def args(self):
        return [broker.Data.to_py(a) for a in _broker.zeek.Event.args(self)]

    def metadata(self):
        """Returns metadata as a list of (count, data) tuples of the event or None."""
        metadata = _broker.zeek.Event.metadata(self)
        if metadata is not None:
            metadata = [broker.Data.to_py(m) for m in metadata]
        return metadata

# Similar to the Subscriber vs SafeSubscriber specialization, this is an event
# specialization that is robust to Python's limitations regarding hashable
# types. If you are working with Zeek types that Python cannot naturally
# repesent (for example, sets of tables, or sets of records with table fields),
# then you want to use this instead of the above Event (and SafeSubscriber
# instead of the regular Subscriber). If you do not, you might hit things like
#
#    File "...python/broker/__init__.py", line 549, in to_set
#        return set([Data.to_py(i) for i in s])
#    TypeError: unhashable type: 'dict'
#
class SafeEvent(Event):
    def args(self):
        return [broker.ImmutableData.to_py(a) for a in _broker.zeek.Event.args(self)]
