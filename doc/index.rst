Broker User Manual
==================

**Broker** is a library for type-rich publish/subscribe communication in
`Bro <https://bro.org>`_'s data model.

Outline
--------

:ref:`overview` introduces Broker's key components and basic terminology,
such as *endpoints*, *messages*, *topics*, and *data stores*.

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
keep its data in various *backends*, currently: in-memory, `SQLite
<https://www.sqlite.org>`_, and `RocksDB <http://rocksdb.org>`_.

Synopsis
--------

.. literalinclude:: _examples/synopsis.cc

.. toctree::
  :numbered:
  :hidden:

  overview
  comm
  data
  stores
