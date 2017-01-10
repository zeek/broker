Broker User Manual
==================

**Broker** is a library for type-rich publish/subscribe communication in
`Bro <https://bro.org>`_'s data model.

Outline
--------

:ref:`overview` introduces Broker's key components and basic terminology,
such as *contexts*, *endpoints*, *messages*, *topics*, and *data stores*.

:ref:`communication` shows how one can send and receive data with Broker's
publish/subscribe communication primitives. By structuring applications in
independent *endpoints* and peering with other endpoints, one can create a
variety of different communication topologies that perform topic-based message
routing.

:ref:`data-model` presents Broker's data model which applications can pack into
messages und publish under a given topic. The same data also works with
Broker's :ref:`data stores <data-stores>`.

:ref:`data-stores` introduces *data stores*, a distributed key-value
abstraction operating with the complete :ref:`data model <data-model>`, for
both keys and values. Users interact with a data store *frontend*, which is
either an authoritative *master* or a *clone* replica. The master can choose to
keep its data in various *backends*: in-memory, `SQLite
<https://www.sqlite.org>`_, and `RocksDB <http://rocksdb.org>`_.

Synopsis
--------

.. code-block:: cpp

  using namespace broker;

  context ctx;
  auto ep = ctx.spawn<nonblocking>(); // create an endpoint
  ep.peer(1.2.3.4, 42000); // peer with a remote endpoint
  ep.publish("/foo", set{1, 2, 3}); // publish data under a given topic
  ep.subscribe("/foo", [=](const topic& t, const data& d) {
    std::cout << "got data for topic " << t << ": " << d << std::endl;
  });

  auto m = ep.attach<master, rocksdb>("yoda", "/tmp/database");
  m.put(4.2, -42);
  m.put("bar", vector{true, 7u, time::now()});
  m.get<nonblocking>(4.2).then(
    [=](const data& d) {
      process(d);
    },
    [=](status s) {
      if (s == ec::key_not_found)
        std::cout << "no such key: 4.2" << std::endl;
      else
        std::terminate();
    }
  );

.. toctree::
  :numbered:
  :hidden:

  overview
  comm
  data
  stores
