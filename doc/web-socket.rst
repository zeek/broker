.. _web-socket:

Web Socket
==========

Broker offers access to the publish/subscribe layer via WebSocket in order to
make its :ref:`data <data-model>` accessible to third parties.

WebSocket clients are treated as lightweight peers. Each Broker endpoint can be
configured to act as a WebSocket server by either setting the environment
variable ``BROKER_WEB_SOCKET_PORT`` or by setting ``broker.web-socket.port`` on
the command line or in the configuration file. When running Zeek, scripts may
also redefine ``Broker::web_socket_port`` to a valid TCP port.

.. note::

  Broker uses the same SSL parameters for native and WebSocket peers.

JSON API v1
-----------

To access the JSON API, clients may connect to
``wss://<host>:<port>/v1/events/json`` (SSL enabled, default) or
``ws://<host>:<port>/v1/events/json`` (SSL disabled). On this WebSocket
endpoint, Broker allows JSON-formatted text messages only.

Handshake
~~~~~~~~~

The broker endpoint expects a JSON array of strings as the first message. This
array encodes the subscriptions as a list of topic prefixes that the client
subscribes to. Clients that only publish data must send an empty JSON array.

After receiving the subscriptions, the Broker endpoint sends a single ACK
message:

.. code-block:: json

  {
    "type": "ack",
    "endpoint": "<uuid>",
    "version": "<broker-version>"
  }

In this message, ``<uuid>`` is the unique endpoint ID of the WebSocket server
and ``<broker-version>`` is a string representation of the ``libbroker``
version, i.e., the result of ``broker::version::string()``. For example:

.. code-block:: json

  {
    "type": "ack",
    "endpoint": "925c9110-5b87-57d9-9d80-b65568e87a44",
    "version": "2.2.0-22"
  }

Protocol
~~~~~~~~

After the handshake, the WebSocket client may only send `Data Messages`_. The
Broker endpoint converts every message to its native representation and
publishes it.

The WebSocket server may send `Event Messages`_ (whenever a data message matches
the subscriptions of the client) and `Error Messages_` to the client.

Data Representation
~~~~~~~~~~~~~~~~~~~

Broker uses a recursive data type to represent its values (see
:ref:`Data Model <data-model>`). This data model does not map to JSON-native
types without ambiguity, e.g., because Broker distinguishes between signed and
unsigned number types.

In JSON, we represent each value as a JSON object with two keys: ``@data-type``
and ``data``. The former identifies one of Broker's data types (see below) and
denotes how Broker parses the ``data`` field.

None
****

There is only exactly one valid input for encoding a ``none``:

.. code-block:: json

  {
    "@data-type": "none",
    "data": {}
  }


Boolean
*******

The type ``boolean`` can take on exactly two values and maps to the native JSON
boolean type:

.. code-block:: json

  {
    "@data-type": "boolean",
    "data": true
  }

.. code-block:: json

  {
    "@data-type": "boolean",
    "data": false
  }

Count
*****

A ``count`` is a 64-bit *unsigned* integer and maps to a (positive) JSON
integer. For example, Broker encodes the count ``123`` as:

.. code-block:: json

  {
    "@data-type": "count",
    "data": 123
  }

.. note::

  Passing a number with a decimal point (e.g. '1.0') is an error.

Integer
*******

The type ``integer`` maps to JSON integers. For example, Broker encodes the
integer ``-7`` as:

.. code-block:: json

  {
    "@data-type": "integer",
    "data": -7
  }

.. note::

  Passing a number with a decimal point (e.g. '1.0') is an error.

Real
****

The type ``real`` maps to JSON numbers. For example, Broker encodes ``-7.5`` as:

.. code-block:: json

  {
    "@data-type": "real",
    "data": -7.5
  }

Timespan
********

A ``timespan`` has no equivalent in JSON and Broker thus encodes them as
strings. The format for the string is ``<value><suffix>``, whereas the *value*
is an integer and *suffix* is one of:

ns
  Nanoseconds.
ms
  Milliseconds.
s
  Seconds.
min
  Minutes
h
  Hours.
d
  Days.

For example, 1.5 seconds may be encoded as:

.. code-block:: json

  {
    "@data-type": "timespan",
    "data": "1500ms"
  }

Timestamp
*********

Like ``timespan``, Broker uses formatted strings to represent ``timestamp``
since there is no native JSON equivalent. Timestamps are encoded in ISO 8601 as
``YYYY-MM-DDThh:mm:ss.sss``.

For example, Broker represents April 10, 2022 at precisely 7AM as:

.. code-block:: json

  {
    "@data-type": "timestamp",
    "data": "2022-04-10T07:00:00.000"
  }

String
******

Strings simply map to JSON strings, e.g.:

.. code-block:: json

  {
    "@data-type": "string",
    "data": "Hello World!"
  }

Enum Value
**********

Broker internally represents enumeration values as strings. Hence, this type
also maps to JSON strings:

.. code-block:: json

  {
    "@data-type": "enum-value",
    "data": "foo"
  }

Address
*******

Network addresses are encoded as strings and use the IETF-recommended string
format for IPv4 and IPv6 addresses, respectively. For example:

.. code-block:: json

  {
    "@data-type": "address",
    "data": "2001:db8::"
  }

Subnet
******

Network subnets are encoded in strings with "slash notation", i.e.,
``<address>/<prefix-length>``. For example:

.. code-block:: json

  {
    "@data-type": "subnet",
    "data": "255.255.255.0/24"
  }

Port
****

Ports are rendered as strings with the format ``<port-number>/<protocol>``,
whereas ``<port-number>`` is a 16-bit unsigned integer and ``protocol`` is one
of ``tcp``, ``udp``, ``icmp``, or ``?``. For example:

.. code-block:: json

  {
    "@data-type": "port",
    "data": "8080/tcp"
  }

Vector
******

A ``vector`` is a sequence of ``data``. This maps to a JSON array consisting of
JSON objects (that in turn each have the ``@data-type`` and ``data`` keys
again). For example:

.. code-block:: json

  "@data-type": "vector",
  "data": [
    {
      "@data-type": "count",
      "data": 42
    },
    {
      "@data-type": "integer",
      "data": 23
    }
  ]


Set
***

Sets are similar to ``vector``, but each object in the list may only appear
once. For example:

.. code-block:: json

  "@data-type": "set",
  "data": [
    {
      "@data-type": "string",
      "data": "foo"
    },
    {
      "@data-type": "string",
      "data": "bar"
    }
  ]

Table
*****

Since Broker allows arbitrary types for the key (even a nested table), Broker
cannot render tables as JSON objects. Hence, tables are mapped JSON arrays of
key-value pairs, i.e., JSON objects with ``key`` and ``value``.
For example:

.. code-block:: json

  {
    "@data-type": "table",
    "data": [
      {
        "key": {
          "@data-type": "string",
          "data": "first-name"
        },
        "value": {
          "@data-type": "string",
          "data": "John"
        }
      },
      {
        "key": {
          "@data-type": "string",
          "data": "last-name"
        },
        "value": {
          "@data-type": "string",
          "data": "Doe"
        }
      }
    ]
  }

Data Messages
~~~~~~~~~~~~~

Broker operates on *data messages* that consist of a *topic* and *data*. In the
JSON representation, this maps to a JSON object with ``topic`` and ``data`` keys
as well as the ``@data-type`` key to encode how to parse the data (see
`Data Representation`_). For example:

.. code-block:: json

  {
    "topic": "/foo/bar",
    "@data-type": "count",
    "data": 1
  }

Event Messages
~~~~~~~~~~~~~~

An event message is a data message with an additional ``type`` key. The value to
this key is always ``event``. For example:

.. code-block:: json

  {
    "type": "event",
    "topic": "/foo/bar",
    "@data-type": "count",
    "data": 1
  }

Error Messages
~~~~~~~~~~~~~~

The error messages on the WebSocket connection give feedback to the client if
the server discarded malformed input from the client or if there has been an
error while processing the JSON text.

An error message consists of these three keys:

``type``
  Always ``error``.

``code``
  A string representation of one of Broker's error codes. See
  :ref:`status-error-messages`.

``context``
  A string that gives additional information as to what went wrong.

For example, sending the server ``How is it going?`` instead of a valid data
message would cause it to send this error back to the client:

.. code-block:: json

  {
    "type": "error",
    "code": "deserialization_failed",
    "context": "input #1 contained malformed JSON -> caf::pec::unexpected_character(1, 1)"
  }
