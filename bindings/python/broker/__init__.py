try:
    from . import _broker
except ImportError:
    import _broker

import collections
import datetime
import ipaddress
import types

try:
    from datetime import timezone

    utc = timezone.utc
except:
    # Only Python 3.2+ has a datetime.timezone.utc we can re-use
    class UTC(datetime.tzinfo):
        def utcoffset(self, dt):
            return datetime.timedelta(0)

        def tzname(self, dt):
            return "UTC"

        def dst(self, dt):
            return datetime.timedelta(0)

        def __eq__(self, other):
            if isinstance(other, UTC):
                return True

            try:
                if self.utcoffset(None) != other.utcoffset(None):
                    return False

                if other.dst(None) is None:
                    return True

                return self.dst(None) == other.dst(None)

            except:
                return False

        def __ne__(self, other):
            return not self.__eq__(other)

    utc = UTC()

Version = _broker.Version
Version.string = lambda: f"{Version.MAJOR}.{Version.MINOR}.{Version.PATCH}"

now = _broker.now

APIFlags = _broker.APIFlags
EC = _broker.EC
SC = _broker.SC
PeerStatus = _broker.PeerStatus
PeerFlags = _broker.PeerFlags
Frontend = _broker.Frontend
Backend = _broker.Backend
NetworkInfo = _broker.NetworkInfo
EndpointInfo = _broker.EndpointInfo
PeerInfo = _broker.PeerInfo
Topic = _broker.Topic
Status = _broker.Status
Error = _broker.Error
Configuration = _broker.Configuration
BrokerOptions = _broker.BrokerOptions

# Broker's (or better: CAF's) EC code is an integer. Add support
# for comparision against the enum.
_EC_eq = _broker.EC.__eq__


def _our_EC_eq(self, other):
    if isinstance(other, int):
        return other == int(self)
    else:
        return _EC_eq(self, other)


_broker.EC.__eq__ = _our_EC_eq

Address = _broker.Address
Count = _broker.Count
Enum = _broker.Enum
Port = _broker.Port
Set = _broker.Set
Subnet = _broker.Subnet
Table = _broker.Table
Timespan = _broker.Timespan
Timestamp = _broker.Timestamp
Vector = _broker.Vector


def _make_topic(t):
    return Topic(t) if not isinstance(t, Topic) else t


def _make_topics(ts):
    if isinstance(ts, Topic):
        ts = [ts]
    elif isinstance(ts, str):
        ts = [Topic(ts)]
    elif isinstance(ts, collections.Iterable):
        ts = [_make_topic(t) for t in ts]
    else:
        ts = [Topic(ts)]

    return _broker.VectorTopic(ts)


# This class does not derive from the internal class because we
# need to pass in existing instances. That means we need to
# wrap all methods, even those that just reuse the internal
# implementation.
class Subscriber:
    def __init__(self, internal_subscriber):
        self._subscriber = internal_subscriber

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._subscriber.reset()
        self._subscriber = None

    def reset(self):
        self._subscriber.reset()
        self._subscriber = None

    def get(self, *args, **kwargs):
        msg = self._subscriber.get(*args, **kwargs)

        if msg is None:
            return None

        if isinstance(msg, _broker.OptionalSubscriberBaseValueType):
            if not msg.is_set():
                return None

            msg = msg.get()

        if isinstance(msg, tuple):
            return (msg[0].string(), Data.to_py(msg[1]))

        if isinstance(msg, _broker.VectorPairTopicData):
            return [(d[0].string(), Data.to_py(d[1])) for d in msg]

        assert False

    def poll(self):
        msgs = self._subscriber.poll()
        return [(d[0].string(), Data.to_py(d[1])) for d in msgs]

    def available(self):
        return self._subscriber.available()

    def fd(self):
        return self._subscriber.fd()

    def add_topic(self, topic, block=False):
        return self._subscriber.add_topic(_make_topic(topic), block)

    def remove_topic(self, topic, block=False):
        return self._subscriber.remove_topic(_make_topic(topic), block)


class SafeSubscriber(Subscriber):
    """Subscriber subclass that makes returnes messages safe to process.

    "Safe" here means safe to Python's type system, particularly regarding
    hashable types. Broker's data model permits nested complex types, such as
    sets of tables, but those don't directly work in Python (for example,
    constructing a set of dicts will complain that dicts aren't hashable). To
    work around this SafeSubscriber relies on ImmutableData rather than Data
    (used by regular Subscribers)."""

    def get(self, *args, **kwargs):
        msg = self._subscriber.get(*args, **kwargs)

        if msg is None:
            return None

        if isinstance(msg, _broker.OptionalSubscriberBaseValueType):
            if not msg.is_set():
                return None

            msg = msg.get()

        if isinstance(msg, tuple):
            return (msg[0].string(), ImmutableData.to_py(msg[1]))

        if isinstance(msg, _broker.VectorPairTopicData):
            return [(d[0].string(), ImmutableData.to_py(d[1])) for d in msg]

        assert False


class StatusSubscriber:
    def __init__(self, internal_subscriber):
        self._subscriber = internal_subscriber

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._subscriber.reset()
        self._subscriber = None

    def reset(self):
        self._subscriber.reset()
        self._subscriber = None

    def get(self, *args, **kwargs):
        x = self._subscriber.get(*args, **kwargs)
        return self._to_result(x)

    def poll(self):
        xs = self._subscriber.poll()
        return [self._to_result(x) for x in xs]

    def available(self):
        return self._subscriber.available()

    def fd(self):
        return self._subscriber.fd()

    def _to_result(self, x):
        if isinstance(x, _broker.VectorStatusSubscriberValueType):
            return [self._to_error_or_status(xi) for xi in x]

        return self._to_error_or_status(x)

    def _to_error_or_status(self, x):
        if x.is_error():
            return x.get_error()

        if x.is_status():
            return x.get_status()

        assert False


class Publisher:
    # This class does not derive from the internal class because we
    # need to pass in existing instances. That means we need to
    # wrap all methods, even those that just reuse the internal
    # implementation.
    def __init__(self, internal_publisher):
        self._publisher = internal_publisher

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._publisher.reset()
        self._publisher = None

    def reset(self):
        self._publisher.reset()
        self._publisher = None

    def demand(self):
        return self._publisher.demand()

    def buffered(self):
        return self._publisher.buffered()

    def capacity(self):
        return self._publisher.capacity()

    def free_capacity(self):
        return self._publisher.free_capacity()

    def send_rate(self):
        return self._publisher.send_rate()

    def fd(self):
        return self._publisher.fd()

    def publish(self, data):
        data = Data.from_py(data)
        return self._publisher.publish(data)

    def publish_batch(self, *batch):
        batch = [Data.from_py(d) for d in batch]
        return self._publisher.publish_batch(_broker.Vector(batch))


class Store:
    # This class does not derive from the internal class because we
    # need to pass in existing instances. That means we need to
    # wrap all methods, even those that just reuse the internal
    # implementation.
    def __init__(self, internal_store):
        self._store = internal_store
        # Points to the "owning" Endpoint to make sure Python cleans this object up
        # before destroying the endpoint.
        self._parent = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._store.reset()
        self._parent = None
        self._store = None

    def name(self):
        return self._store.name()

    def exists(self, key):
        key = Data.from_py(key)
        value = self._store.exists(key)
        return Data.to_py(value.get())

    def get(self, key):
        key = Data.from_py(key)
        value = self._store.get(key)
        return Data.to_py(value.get()) if value.is_valid() else None

    def get_index_from_value(self, key, index):
        key = Data.from_py(key)
        index = Data.from_py(index)
        value = self._store.get_index_from_value(key, index)
        return Data.to_py(value.get()) if value.is_valid() else None

    def keys(self):
        keys = self._store.keys()

        if keys is None:
            return None

        return Data.to_py(keys.get()) if keys.is_valid() else None

    def put(self, key, value, expiry=None):
        key = Data.from_py(key)
        value = Data.from_py(value)
        expiry = self._to_expiry(expiry)
        return self._store.put(key, value, expiry)

    def put_unique(self, key, value, expiry=None):
        key = Data.from_py(key)
        value = Data.from_py(value)
        expiry = self._to_expiry(expiry)
        rval = self._store.put_unique(key, value, expiry)
        return Data.to_py(rval.get()) if rval.is_valid() else None

    def erase(self, data):
        data = Data.from_py(data)
        return self._store.erase(data)

    def clear(self):
        return self._store.clear()

    def increment(self, key, amount, expiry=None):
        key = Data.from_py(key)
        amount = Data.from_py(amount)
        expiry = self._to_expiry(expiry)
        return self._store.increment(key, amount, expiry)

    def decrement(self, key, amount, expiry=None):
        key = Data.from_py(key)
        amount = Data.from_py(amount)
        expiry = self._to_expiry(expiry)
        return self._store.decrement(key, amount, expiry)

    def append(self, key, s, expiry=None):
        key = Data.from_py(key)
        s = Data.from_py(s)
        expiry = self._to_expiry(expiry)
        return self._store.append(key, s, expiry)

    def insert_into(self, key, index, value=None, expiry=None):
        key = Data.from_py(key)
        index = Data.from_py(index)
        expiry = self._to_expiry(expiry)

        if value:
            value = Data.from_py(value)
            return self._store.insert_into(key, index, value, expiry)
        else:
            return self._store.insert_into(key, index, expiry)

    def remove_from(self, key, index, expiry=None):
        key = Data.from_py(key)
        index = Data.from_py(index)
        expiry = self._to_expiry(expiry)
        return self._store.remove_from(key, index, expiry)

    def push(self, key, value, expiry=None):
        key = Data.from_py(key)
        value = Data.from_py(value)
        expiry = self._to_expiry(expiry)
        return self._store.push(key, value, expiry)

    def pop(self, key, expiry=None):
        key = Data.from_py(key)
        expiry = self._to_expiry(expiry)
        return self._store.pop(key, expiry)

    def _to_expiry(self, e):
        return (
            _broker.OptionalTimespan(_broker.Timespan(float(e)))
            if e is not None
            else _broker.OptionalTimespan()
        )

    def await_idle(self, timeout=None):
        if timeout:
            return self._store.await_idle(_broker.Timespan(float(timeout)))
        else:
            return self._store.await_idle()

    # Points to the "owning" Endpoint to make sure Python cleans this object up
    # before destroying the endpoint.
    _parent = None


class Endpoint(_broker.Endpoint):
    def make_subscriber(self, topics, qsize=20, subscriber_class=Subscriber):
        topics = _make_topics(topics)
        s = _broker.Endpoint.make_subscriber(self, topics, qsize)
        return subscriber_class(s)

    def make_safe_subscriber(self, topics, qsize=20):
        """A variant of make_subscriber that returns a SafeSubscriber instance. In
        contrast to the Subscriber class, messages retrieved from
        SafeSubscribers use immutable, hashable values to ensure Python can
        represent them. When in doubt, use make_safe_subscriber()."""
        return self.make_subscriber(
            topics=topics, qsize=qsize, subscriber_class=SafeSubscriber
        )

    def make_status_subscriber(self, receive_statuses=False):
        s = _broker.Endpoint.make_status_subscriber(self, receive_statuses)
        return StatusSubscriber(s)

    def make_publisher(self, topic):
        topic = _make_topic(topic)
        p = _broker.Endpoint.make_publisher(self, topic)
        return Publisher(p)

    def forward(self, topics):
        topics = _make_topics(topics)
        _broker.Endpoint.forward(self, topics)

    def publish(self, topic, data):
        topic = _make_topic(topic)
        data = Data.from_py(data)
        return _broker.Endpoint.publish(self, topic, data)

    def publish_batch(self, *batch):
        batch = [(_make_topic(t), Data.from_py(d)) for (t, d) in batch]
        return _broker.Endpoint.publish_batch(self, _broker.VectorPairTopicData(batch))

    def attach_master(self, name, type=None, opts={}):
        bopts = _broker.MapBackendOptions()  # Generator expression doesn't work here.
        for k, v in opts.items():
            bopts[k] = Data.from_py(v)
        s = _broker.Endpoint.attach_master(self, name, type, bopts)
        if not s.is_valid():
            return None
        result = Store(s.get())
        # This extra reference establishes a parent-child relation between the
        # two Python objects, making sure that the garbage collector destroys
        # the store *before* cleaning up the endpoint
        result._parent = self
        return result

    def attach_clone(self, name):
        s = _broker.Endpoint.attach_clone(self, name)
        if not s.is_valid():
            return None
        result = Store(s.get())
        # Same as above: make sure Python cleans up the store first.
        result._parent = self
        return result

    def await_peer(self, node, timeout=None):
        if timeout:
            return _broker.Endpoint.await_peer(
                self, node, _broker.Timespan(float(timeout))
            )
        else:
            return _broker.Endpoint.await_peer(self, node)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.shutdown()


class Message:
    def to_broker(self):
        assert False and "method not overridden"


from . import zeek


class Data(_broker.Data):
    def __init__(self, x=None):
        if x is None:
            _broker.Data.__init__(self)

        elif isinstance(x, zeek.Event):
            _broker.Data.__init__(self, x.as_data())

        elif isinstance(x, _broker.Data):
            _broker.Data.__init__(self, x)

        elif isinstance(
            x,
            (
                bool,
                int,
                float,
                str,
                bytes,
                Address,
                Count,
                Enum,
                Port,
                Set,
                Subnet,
                Table,
                Timespan,
                Timestamp,
                Vector,
            ),
        ):
            _broker.Data.__init__(self, x)

        elif isinstance(x, datetime.timedelta):
            us = x.microseconds + (x.seconds + x.days * 24 * 3600) * 10**6
            ns = us * 10**3
            _broker.Data.__init__(self, _broker.Timespan(ns))

        elif isinstance(x, datetime.datetime):
            secs = x.timestamp()
            _broker.Data.__init__(self, _broker.Timestamp(secs))

        elif isinstance(x, ipaddress.IPv4Address):
            _broker.Data.__init__(self, _broker.Address(x.packed, 4))

        elif isinstance(x, ipaddress.IPv6Address):
            _broker.Data.__init__(self, _broker.Address(x.packed, 6))

        elif isinstance(x, ipaddress.IPv4Network):
            address = _broker.Address(x.network_address.packed, 4)
            length = x.prefixlen
            _broker.Data.__init__(self, _broker.Subnet(address, length))

        elif isinstance(x, ipaddress.IPv6Network):
            address = _broker.Address(x.network_address.packed, 6)
            length = x.prefixlen
            _broker.Data.__init__(self, _broker.Subnet(address, length))

        elif isinstance(x, list) or isinstance(x, tuple):
            v = _broker.Vector([Data(i) for i in x])
            _broker.Data.__init__(self, v)

        elif isinstance(x, set) or isinstance(x, frozenset):
            s = _broker.Set([Data(i) for i in x])
            _broker.Data.__init__(self, s)

        elif isinstance(x, dict) or isinstance(x, types.MappingProxyType):
            t = _broker.Table()
            for k, v in x.items():
                t[Data(k)] = Data(v)

            _broker.Data.__init__(self, t)

        else:
            raise TypeError("unsupported data type: " + str(type(x)))

    @staticmethod
    def from_py(x):
        return Data(x)

    @staticmethod
    def to_py(d):
        def to_ipaddress(a):
            if a.is_v4():
                return ipaddress.IPv4Address(a.bytes()[-4:])
            else:
                return ipaddress.IPv6Address(a.bytes())

        def to_subnet(s):
            # Python < 3.5 does not have a nicer way of setting the prefixlen
            # when creating from packed data.
            if s.network().is_v4():
                return ipaddress.IPv4Network(to_ipaddress(s.network())).supernet(
                    new_prefix=s.length()
                )
            else:
                return ipaddress.IPv6Network(to_ipaddress(s.network())).supernet(
                    new_prefix=s.length()
                )

        def to_set(s):
            return {Data.to_py(i) for i in s}

        def to_table(t):
            return {Data.to_py(k): Data.to_py(v) for (k, v) in t.items()}

        def to_vector(v):
            return tuple(Data.to_py(i) for i in v)

        def _try_bytes_decode(b):
            try:
                return b.decode("utf-8")
            except:
                return b

        converters = {
            Data.Type.Nil: lambda: None,
            Data.Type.Address: lambda: to_ipaddress(d.as_address()),
            Data.Type.Boolean: lambda: d.as_boolean(),
            Data.Type.Count: lambda: Count(d.as_count()),
            Data.Type.EnumValue: lambda: d.as_enum_value(),
            Data.Type.Integer: lambda: d.as_integer(),
            Data.Type.Port: lambda: d.as_port(),
            Data.Type.Real: lambda: d.as_real(),
            Data.Type.Set: lambda: to_set(d.as_set()),
            Data.Type.String: lambda: _try_bytes_decode(d.as_string()),
            Data.Type.Subnet: lambda: to_subnet(d.as_subnet()),
            Data.Type.Table: lambda: to_table(d.as_table()),
            Data.Type.Timespan: lambda: datetime.timedelta(seconds=d.as_timespan()),
            Data.Type.Timestamp: lambda: datetime.datetime.fromtimestamp(
                d.as_timestamp(), utc
            ),
            Data.Type.Vector: lambda: to_vector(d.as_vector()),
        }

        try:
            return converters[d.get_type()]()
        except KeyError:
            raise TypeError("unsupported data type: " + str(d.get_type()))


class ImmutableData(Data):
    """A Data specialization that uses immutable complex types for returned Python
    objects. For sets, the return type is frozenset, for tables it's a hashable,
    read-only derivative of dict, and for vectors it's Python tuples.
    """

    class HashableReadOnlyDict(dict):
        def __hash__(self):
            return hash(frozenset(self.items()))

        def __readonly__(self, *args, **kwargs):
            raise TypeError("cannot modify this dict")

        # https://stackoverflow.com/a/31049908
        __setitem__ = __readonly__
        __delitem__ = __readonly__
        pop = __readonly__
        popitem = __readonly__
        clear = __readonly__
        update = __readonly__
        setdefault = __readonly__
        del __readonly__

    @staticmethod
    def to_py(d):
        def to_set(s):
            return frozenset([ImmutableData.to_py(i) for i in s])

        def to_table(t):
            tmp = {
                ImmutableData.to_py(k): ImmutableData.to_py(v) for (k, v) in t.items()
            }
            # It's tempting here to use types.MappingProxyType here, but it is
            # not hashable, so doesn't solve our main problem, and cannot be
            # derived from.
            return ImmutableData.HashableReadOnlyDict(tmp.items())

        def to_vector(v):
            return tuple(ImmutableData.to_py(i) for i in v)

        converters = {
            Data.Type.Set: lambda: to_set(d.as_set()),
            Data.Type.Table: lambda: to_table(d.as_table()),
            Data.Type.Vector: lambda: to_vector(d.as_vector()),
        }

        try:
            return converters[d.get_type()]()
        except KeyError:
            # Fall back on the Data class for types we handle identically.
            return Data.to_py(d)


####### TODO: Updated to new Broker API until here.

# # TODO: complete interface
# class Store:
#   def __init__(self, handle):
#     self.store = handle
#
#   def name(self):
#     return self.store.name()
#
# class Mailbox:
#   def __init__(self, handle):
#     self.mailbox = handle
#
#   def descriptor(self):
#     return self.mailbox.descriptor()
#
#   def empty(self):
#     return self.mailbox.empty()
#
#   def count(self, n = -1):
#     return self.mailbox.count(n)
#
#
# class Message:
#   def __init__(self, handle):
#     self.message = handle
#
#   def topic(self):
#     return self.message.topic().string()
#
#   def data(self):
#     return self.message.data() # TODO: unwrap properly
#
#   def __str__(self):
#     return "%s -> %s" % (self.topic(), str(self.data()))
#
#
# class BlockingEndpoint(Endpoint):
#   def __init__(self, handle):
#     super(BlockingEndpoint, self).__init__(handle)
#
#   def subscribe(self, topic):
#     self.endpoint.subscribe(topic)
#
#   def unsubscribe(self, topic):
#     self.endpoint.unsubscribe(topic)
#
#   def receive(self, x):
#     if x == Status:
#       return self.endpoint.receive()
#     elif x == Message:
#       return Message(self.endpoint.receive())
#     else:
#       raise BrokerError("invalid receive type")
#
#   #def receive(self):
#   #  if fun1 is None:
#   #    return Message(self.endpoint.receive())
#   #  if fun2 is None:
#   #    if utils.arity(fun1) == 1:
#   #      return self.endpoint.receive_status(fun1)
#   #    if utils.arity(fun1) == 2:
#   #      return self.endpoint.receive_msg(fun1)
#   #    raise BrokerError("invalid receive callback arity; must be 1 or 2")
#   #  return self.endpoint.receive_msg_or_status(fun1, fun2)
#
#   def mailbox(self):
#     return Mailbox(self.endpoint.mailbox())
#
#
# class NonblockingEndpoint(Endpoint):
#   def __init__(self, handle):
#     super(NonblockingEndpoint, self).__init__(handle)
#
#   def subscribe(self, topic, fun):
#     self.endpoint.subscribe_msg(topic, fun)
#
#   def on_status(fun):
#     self.endpoint.subscribe_status(fun)
#
#   def unsubscribe(self, topic):
#     self.endpoint.unsubscribe(topic)
#
#
# class Context:
#   def __init__(self):
#     self.context = _broker.Context()
#
#   def spawn(self, api):
#     if api == Blocking:
#       return BlockingEndpoint(self.context.spawn_blocking())
#     elif api == Nonblocking:
#       return NonblockingEndpoint(self.context.spawn_nonblocking())
#     else:
#       raise BrokerError("invalid API flag: " + str(api))
#
