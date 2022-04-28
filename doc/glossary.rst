Glossary
========

.. glossary::

  Message

    A message consists of a ``broker::topic`` and a ``broker::data``. Broker
    stores messages as copy-on-write tuples (``broker::data_message``). This
    allows Broker to pass messages to many receivers without having to copy the
    content for each subscriber.

  Filter

    Each endpoint (see :ref:`endpoint`) controls the amount of data it receives
    from others by providing a list of topic prefixes. Whenever an endpoint
    publishes data, this list (the filter) is used to determine which peering
    endpoint should receive the data. For example, if the endpoints A and B have
    a peering relationship and B has announced the filter ``[/zeek/events/123/,
    /zeek/events/234/]`` then A would forward messages for the topic
    ``/zeek/events/123/foo`` to B, while not forwarding messages for the topic
    ``/zeek/events/456/foo``.
