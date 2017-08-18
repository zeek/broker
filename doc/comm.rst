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
communicate in Bro's data model. In this chapter, we describe first
describe generic Broker communication between peers that don't assume
any specific message layout. Afterwards, we show how to exchange
events with Bro through an additional Bro-specific shim on top of
Broker's generic messages.

Endpoints
---------

Broker encapsulates its entire peering in an ``endpoint`` object. Multiple
instances of a ``endpoint`` can exist in the same process, but each ``endpoint``
features a thread-pool and (configurable) scheduler, which determines the
execution of Broker's components. Using a single ``endpoint`` per OS process
guarantees the most efficient usage of available hardware resources.
Nonetheless, multiple Broker applications can seamlessly operate when linked
together, as there exists no global library state.

.. note::

  Instances of type ``endpoint`` have reference semantics: that is, they behave
  like a reference in that it's impossible to obtain an invalid one (unlike a
  null pointer). An ``endpoint`` can also be copied around cheaply, but is not
  thread-safe.


Receiving Data
~~~~~~~~~~~~~~

Endpoints receive data by creating a ``subscriber`` attached to the
topics of interest. Subscriptions are prefix-based, matching all
topics that start with the guven string. A ``subscriber`` can either
retrieve incoming messages explicitly by calling ``get`` or ``poll``
(synchronous API), or spawning a background worker to process messages
as they come in (asynchronous API).

Synchronous API
***************
    
The synchronous API exists for applications that want to poll for
messages explicitly. Once a subscribers is registered for topics,
calling ``get`` will wait for a new message:

.. literalinclude:: _examples/comm.cc
   :start-after: --get-start
   :end-before: --get-end

The function ``get`` blocks until the subscriber has at least one
message available. Each message has two parts: the topic is was
published to, and the message's payload in the form of a `data`
instance. The example just prints them out.

Blocking indefinitely does not work well in combination with existing
event loops or polling. `get` takes an optional timeout parameter to
wait only for a certain amount of time. One can also use ``available``
to explicitly check for available messages, or ``poll`` to extract all
currently pending messages (which may be none):

.. literalinclude:: _examples/comm.cc
   :start-after: --poll-start
   :end-before: --poll-end

For integration into event loops, `subscriber` also provides a file
descriptor that signals available messages:

.. literalinclude:: _examples/comm.cc
   :start-after: --fd-start
   :end-before: --fd-end

Asynchronous API
****************

TODO.

.. If your application does not require a blocking API, the non-blocking API
.. offers an asynchronous alternative. Unlike the blocking API, non-blocking
.. endpoints take a callback for each topic they subscribe to:
.. 
.. .. code-block:: cpp
.. 
..   context ctx;
..   auto ep = ctx.spawn<nonblocking>();
..   ep.subscribe("/foo", [=](const topic& t, const data& d) {
..     std::cout << t << " -> " << d << std::endl;
..   });
..   ep.subscribe("/bar", [=](const topic& t, const data& d) {
..     std::cout << t << " -> " << d << std::endl;
..   });
.. 
.. When a new message matching the subscription arrives, Broker dispatches it to
.. the callback without blocking.
.. 
.. .. warning::
.. 
..   The function ``subscribe`` returns immediately. Capturing variable *by
..   reference* introduces a dangling reference once the outer frame returns.
..   Therefore, only capture locals *by value*.

Sending Data
~~~~~~~~~~~~

The API for sending data is the same for blocking and non-blocking endpoints.
In Broker, a *message* is a *topic*-*data* pair. That is, endpoints *publish*
data under a *topic* to send a message to all subscribers:

.. literalinclude:: _examples/comm.cc
   :start-after: --publish-start
   :end-before: --publish-end

.. note::

  Publishing a message can be no-op if there exists no subscriber. Because
  Broker has fire-and-forget messaging semantics, the runtime does not generate
  a notification if no subscribers exist.


One can also explicitly create a ``publisher`` for a specific topic
first, and then use that to send subsequent messages:

.. literalinclude:: _examples/comm.cc
   :start-after: --publisher-start
   :end-before: --publisher-end

This approach better suited for high-volume streams, as it leverages
CAF's demand management internally.

Finally, there's also a streaming version that pulls messages from a
producer as capacity becomes available on the output channel; see
``endpoint::publish_all`` and ``endpoint::publish_all_no_sync``.

See :ref:`data-model` for a detailed discussion on how to construct various
types of ``data``.

Peerings
--------

In order to publish messages to elsewhere, an endpoint needs to
peer with other endpoints. A peering is a bidirectional relationship between
two endpoints. Peering endpoints exchange subscriptions and forward messages
accordingly. This allows for creating flexible communication topologies that
use topic-based message routing.

An endpoint can either create peering itself by connecting to remote
locations, or wait for an incoming request:

.. literalinclude:: _examples/comm.cc
   :start-after: --peering-start
   :end-before: --peering-end

.. note::

    Currently subscriptions are not propagated across hops, and Broker
    does not yet route messages. Support for topic-based routing will
    come soon.


.. _status-error-messages:

Status and Error Messages
-------------------------

Broker presents runtime connective changes to the user as ``status``
messages. For failures, there exists a separate ``error`` type. To get
access to either one creates a ``status_subscriber``, which provides a
similar synchronous ``get/available/poll`` API as the standard message
subscriber. By default, a ``status_subscriber`` returns only errors:

.. literalinclude:: _examples/comm.cc
   :start-after: --status-subscriber-err-start
   :end-before: --status-subscriber-err-end

Errors reflect failures that may impact the correctness of operation.
`err.code()`` returns an enum ``ec`` that codifies existing error
codes:

.. literalinclude:: ../broker/error.hh
   :language: cpp
   :lines: 23-48

To receive non-critical status messages as well, specify that when
creating the ``status_subscriber``:

.. literalinclude:: _examples/comm.cc
   :start-after: --status-subscriber-all-start
   :end-before: --status-subscriber-all-end

Status messages represent non-critical changes to the topology. For
example, after a successful peering, both endpoints receive a
``peer_added`` status message. The concrete semantics of a status
depend on its embedded code, which the enum ``sc`` codifies:

.. literalinclude:: ../broker/status.hh
   :language: cpp
   :lines: 26-35

Status messages have an optional *context* and an optional descriptive
*message*. The member function ``context<T>`` returns a ``const T*``
if the context is available. The type of available context information
is dependent on the status code enum ``sc``. For example, all
``sc::peer_*`` status codes include an ``endpoint_info`` context as
well as a message.

