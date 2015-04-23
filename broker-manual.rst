===============================
Broker: Bro's Messaging Library
===============================

Introduction
============

The Broker library is the successor to the Broccoli library.  One of the
main differences is that while Broccoli is a reimplementation of a
communication protocol built in to Bro (i.e. Bro does *not* link against
libbroccoli), Broker implements a standalone protocol and data model
that Bro uses directly (i.e. Bro *does* link against libbroker).  All
applications that integrate the Broker library share the same
capabilities for exchanging information with each other, so let's go
through some examples of what you can do.

Creating Endpoints and Peering Them Together
============================================

Connecting endpoints together is simple, just give them descriptive
names and request they peer with each other.

.. code:: c++

    #include <broker/broker.hh>
    #include <broker/endpoint.hh>

    int main()
        {
        broker::init();
        broker::endpoint first_endpoint("1st endpoint");
        broker::endpoint second_endpoint("2nd endpoint");
        second_endpoint.peer(first_endpoint);
        return 0;
        }

Endpoints don't have to live in the same process, instead they can talk
over TCP.

.. code:: c++

    broker::endpoint first_endpoint("1st endpoint");
    first_endpoint.listen(9999, "127.0.0.1");

.. code:: c++

    broker::endpoint second_endpoint("2nd endpoint");
    second_endpoint.peer("127.0.0.1", 9999);

The initiator of a remote peering will automatically try reconnecting if
it cannot initially reach the remote side or if it ever becomes
disconnected.  The frequency at which it retries can be passed as the
third parameter to the ``peer()`` call.

Sending Messages
================

Now that endpoints are connected, they can talk to each other.

.. code:: c++

    first_endpoint.send("my_topic", broker::message{"hi", 42});

Any endpoints connected to "first_endpoint" will receive the message if
they have advertised interest (covered below) in any prefix of
"my_topic".

Broker messages are a sequence of data items and a data item is a
variant type whose storage may hold one of several data types.  In the
above code, the message contains the string, "hi", and an unsigned
64-bit integer, 42.  For all the possible data types allowed in Broker
data/messages see the data API reference.

The third paramenter to the ``send()`` method are flags which tune the
behavior of how the message is to be sent.  By default, messages are
allowed to be sent to the sending endpoint itself or to peer
endpoints if they advertise interest in any prefix of the message's
topic string.  There's another flag, ``UNSOLICITED`` that can be used to
send messages to peers even if they have not advertised interest in any
prefix of the message's topic string.

Receiving Messages
==================

To receive messages from other endpoints, one needs to create a message
queue which advertises interest in a topic prefix and then extract
messages from the queue.

.. code:: c++

    broker::message_queue my_queue("my", second_endpoint);

Here, the message queue is attached to the "second_endpoint" endpoint
and subscribes to all messages that get sent with a topic string
prefixed by "my".  Message subscriptions are prefix-based, e.g. the
empty string will match messages sent with any topic and "a" will
receive messages sent with either topic "alice" or "amy" but not "bob".

Broker's queue structures expose a file descriptor that signals
read-readiness when the queue has content that can be retrieved.
It can be used to integrate into event loops as usual.

.. code:: c++

    pollfd pfd{my_queue.fd(), POLLIN, 0};
    poll(&pfd, 1, -1);

    for ( auto& msg : my_queue.want_pop() )
        std::cout << broker::to_string(msg) << std::endl;

Alternatively, there is a ``need_pop()`` method which blocks until
at least one item is available in the queue.  This is mostly for
convenience, use with caution.

Either pop method retrieves all contents that have been received by the
queue up to that point in time.

Monitor Connection Status
=========================

By default, Broker endpoints have queues attached to them which can be
monitored to check the status of connections with peer endpoints.

.. code:: c++

    broker::endpoint node0("node0");
    broker::endpoint node1("node1");
    broker::endpoint node2("node2");
    node0.peer(node1);
    node0.peer(node2);

    for ( ; ; )
        {
        auto conn_status = node0.outgoing_connection_status().need_pop();

        for ( auto cs : conn_status )
            if ( cs.status == broker::outgoing_connection_status::tag::established )
                std::cout << "established connection to: " << cs.peer_name << std::endl;
            else
                std::cout << "connection error" << std::endl;
        }

Applications should periodically check connection status queues for
updates.

Tuning Access Control
=====================

By default, Broker endpoints do not restrict the message topics that it
sends to peers and do not restrict what message queue topics and data
store identifiers get advertised to peers.  This is the default
``AUTO_PUBLISH | AUTO_ADVERTISE`` flags argument to the ``endpoint``
constructor.

If not using the ``AUTO_PUBLISH`` flag, one can use an endpoint's
``publish()`` and ``unpublish()`` methods to manipulate the set of
message topics (must match exactly) that are allowed to be sent to peer
endpoints.  These settings take precedence over the per-message
``PEERS`` flag supplied to ``send()``.

If not using the ``AUTO_ADVERTISE`` flag, one can use an endpoint's
``advertise()`` and ``unadvertise()`` to manipulate the set of topic
prefixes that are allowed to be advertised to peers.  If an endpoint
does not advertise a topic prefix, then the only way peers can send
messages to it is via the ``UNSOLICITED`` flag to ``send()`` and
choosing a topic with a matching prefix (i.e. full topic may be longer
than receivers prefix, just the prefix needs to match).

Distributed Data Stores
=======================

There are three flavors of key-value data store interfaces: master,
clone, and frontend.

A frontend is the common interface to query and modify data stores.
That is, a clone is a specific type of frontend and a master is also a
specific type of frontend, but a standalone frontend can also exist to
e.g. query and modify the contents of a remote master store without
actually "owning" any of the contents itself.

A master data store attached with one Broker endpoint can be cloned
at peer endpoints which may then perform lightweight, local queries
against the clone, which automatically stays synchronized with the
master store.  Clones cannot modify their content directly, instead they
send modifications to the centralized master store which applies them
and then broadcasts them to all clones.

Master and clone stores get to choose what type of storage backend to
use.  E.g. In-memory versus SQLite for persistence.  Note that if clones
are used, data store sizes should still be able to fit within memory
regardless of the storage backend as a single snapshot of the master
store is sent in a single chunk to initialize the clone.

Data stores also support expiration on a per-key basis either using an
absolute point in time or a relative amount of time since the entry's
last modification time.

See the unit tests in ``tests/test_store*`` and the ``store/`` API
reference for more examples and details.
