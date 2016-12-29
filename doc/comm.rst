.. _communication:

Communication
=============

Broker's primary objective is to facilitate efficient communication through a
publish/subscribe model. In this model, entities send data by publishing to a
specific topic, and receive data by subscribing to a topic of interest. The
asynchronous nature of publish/subscribe makes it a popular choice for loosely
coupled, distributed systems.

Broker is the successor of `Broccoli
<https://www.bro.org/sphinx/components/broccoli/broccoli-manual.html>`_, Bro's
client communications library. Broccoli enables arbitrary applications to
communicate in Bro's data model.

Endpoints
---------

Broker encapsulates its entire state in a ``context`` object. Multiple
instances of a ``context`` can exist in the same process, but each ``context``
features a thread-pool and (configurable) scheduler, which determines the
execution of Broker's components. Using a single ``context`` per OS process
guarantees the most efficient usage of available hardware resources.
Nonetheless, multiple Broker applications can seamlessly operate when linked
together, as there exists no global library state.

A ``context`` can *spawn* ``endpoint`` instances, of which there exists a
*blocking* and *non-blocking* variant. The two types differ in the way they
manage their subscriptions and receive messages. Both variants have a
*mailbox*, which is essentially a queue with its unprocessed messages. In the
blocking case, the user manually extracts messages from the mailbox, whereas
the Broker runtime dispatches messages asynchronously in the non-blocking case.

Both endpoint variants can be mixed and matched, it is not necessary to commit
to a particular type for all endpoints within a context.

.. note::

  Instances of type ``endpoint`` have reference semantics: that is, they behave
  like a reference in that it's impossible to obtain an invalid one (unlike a
  null pointer). An ``endpoint`` can also be copied around cheaply, but is not
  thread-safe.

Receiving Data
~~~~~~~~~~~~~~

Endpoints can receive data through an explicit call to ``receive`` (blocking
API) or installing a callback invoked by the runtime (non-blocking API).

Blocking
********

The blocking API exists for applications that primarily operate synchronously
and/or ship their own event loop. Endpoints subscribe to various topics and
call a ``receive`` function to block and wait for message:

.. code-block:: cpp

  context ctx;
  auto ep = ctx.spawn<blocking>();
  ep.subscribe("foo");
  auto msg = ep.receive(); // block and wait until a message arrives
  if (msg)
    std::cout << msg.topic() << " -> " << msg.data() << std::endl;
  else
    std::cout << to_string(msg.status()) << std::endl;

The function ``receive`` blocks until the endpoint receives a ``message``,
which is either a (``topic``, ``data``) pair or a ``status`` to signal an error
or a status change of the endpoint topology. More on ``status``
handling in :ref:`status-handling`.

Blocking indefinitely does not work well in combination with existing event
loops or polling. Therefore, blocking endpoints offer an additional ``mailbox``
API:

.. code-block:: cpp

  // Manual polling.
  if (!ep.mailbox().empty()) {
    auto msg = ep.receive(); // guaranteed to not block
    ...
  }

  // Introspection.
  auto n = ep.mailbox().count(); // O(n) due to internal queue implementation

  // Event loop integration.
  auto fd = ep.mailbox().descriptor();
  ::pollfd p = {fd, POLLIN, {}};
  auto n = ::poll(&p, 1, timeout);
  if (n < 0)
    std::terminate(); // poll failed
  if (n == 1 && p.revents & POLLIN) {
    auto msg = ep.receive(); // guaranteed to not block
    ...
  }


Non-Blocking
************

If your application does not require a blocking API, the non-blocking API
offers an asynchronous alternative. Unlike the blocking API, non-blocking
endpoints take a callback for each topic they subscribe to:

.. code-block:: cpp

  context ctx;
  auto ep = ctx.spawn<nonblocking>();
  ep.subscribe("/foo", [=](const topic& t, const data& d) {
    std::cout << t << " -> " << d << std::endl;
  });
  ep.subscribe("/bar", [=](const topic& t, const data& d) {
    std::cout << t << " -> " << d << std::endl;
  });

When a new message matching the subscription arrives, Broker dispatches it to
the callback without blocking.

.. warning::

  The function ``subscribe`` returns immediately. Capturing variable *by
  reference* introduces a dangling reference once the outer frame returns.
  Therefore, only capture locals *by value*.

Sending Data
~~~~~~~~~~~~

The API for sending data is the same for blocking and non-blocking endpoints.
In Broker, a *message* is a *topic*-*data* pair. That is, endpoints *publish*
data under a *topic* to send a message to all subscribers:

.. code-block:: cpp

  ep.publish("foo", 42);
  ep.publish("bar", vector{1, 2, 3});
  ep.publish("baz", 1, 2, 3); // same as above, implicit conversion to vector

The one-argument version of ``publish`` takes as first argument a ``topic`` and
``data`` instance. The variadic version implicitly constructs a ``vector`` from
the provided ``data`` instances.

.. note::

  Publishing a message can be no-op if there exists no subscriber. Because
  Broker has fire-and-forget messaging semantics, the runtime does not generate
  a notification if no subscribers exist.

See :ref:`data-model` for a detailed discussion on how to construct various
types of ``data``.

Peerings
--------

In order to publish messages beyond the sending endpoint, an endpoint needs to
peer with other endpoints. A peering is a bidirectional relationship between
two endpoints. Peering endpoints exchange subscriptions and forward messages
accordingly. This allows for creating flexible communication topologies that
use topic-based message routing.

.. note::

  Broker currently does not support topologies with loops. This is purely a
  technical limitation and vanishes in the future.

An endpoint can either peer with a local or a remote endpoint:

.. code-block:: cpp

  context ctx;
  auto ep0 = ctx.spawn<blocking>();
  ep0.subscribe("foo");
  auto ep1 = ctx.spawn<nonblocking>();
  ep0.peer(ep1); // exchanges existing subscriptions
  ep1.subscribe("bar"); // relays subscription to peers

The figure below shows the subscription before and after entering the peering
relationship.

.. image:: _images/peering-1.png
  :align: center

Let's consider a third endpoint joining, this time through a remote connection.

.. code-block:: cpp

  // Expose endpoint at an IP address and TCP port.
  ep0.listen(1.2.3.4, 40000);

  // In a separate OS process, connect to it.
  context ctx;
  auto ep2 = ctx.spawn<nonblocking>();
  ep2.peer(1.2.3.4, 42000); // installs a remote peering

Thereafter, we have the following topology:

.. image:: _images/peering-2.png
  :align: center

Note that ``ep2`` does not know about ``ep1`` and forwards ``data`` for topic
``foo`` and ``bar`` via ``ep0``. However, ``ep2.publish("bar", 42)`` still
forwards a message via ``ep0`` to ``ep1``.

.. _status-handling:

Status and Error Handling
-------------------------

In an distributed system, failures occur routinely. While Broker cannot prevent
these events from happening, it presents to the user in the form of ``status``
messages. Blocking endpoints get them via ``receive`` and non-blocking
endpoints much subscribe to them explicitly.

For example, when a new peering relationship gets estalbished, both endpoints
receive ``peer_added`` status message:

.. code-block:: cpp

  context ctx;
  // Peer two endpoints.
  auto ep0 = ctx.spawn<blocking>();
  auto ep1 = ctx.spawn<blocking>();
  ep0.peer(ep1);
  // Block and wait for status messages.
  auto msg0 = ep0.receive();
  assert(!msg0);
  if (msg0.status() == sc::peer_added)
    std::cout << "peering established successfully" << std::endl;

An instance of type ``status`` is equality-comparable with one of the status
codes of the ``sc`` enum. For example, ``sc::peer_added`` conveyes a successful
peering.

Status messages have an optional *context* and an optional descriptive
*message*:

.. code-block:: cpp

  auto& s = msg.status();
  // Check for textual description.
  if (auto str = s.message())
    std::cout << *str << std::endl;
  // Check for contextual information.
  if (auto ctx = s.context<endpoint_info>())
    if (ctx->network)
      std::cout << "peer at: "
                << ctx->network->address << ':'
                << ctx->network->port << std::endl;

The member function ``context<T>`` returns a ``const T*`` if the context is
available. The type of available context information is dependent on the status
code enum ``sc``. For example, all ``sc::peer_`` status codes have an
``enpoint_info`` context as well as a message.

For nonblocking endpoints, status message get ignored unless subscribing to
them explicitly:

.. code-block:: cpp

  context ctx;
  auto ep = ctx.spawn<nonblocking>();
  ep.subscribe(
    [&](const status& s) { ... }
  );
