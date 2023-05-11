.. _web-socket:

Web Socket
==========

Broker offers access to the publish/subscribe layer via WebSocket in order to
make its :ref:`data <data-model>` accessible to third parties.

WebSocket clients are treated as lightweight peers. Each Broker
endpoint can be configured to act as a WebSocket server by either (1)
setting the environment variable ``BROKER_WEB_SOCKET_PORT``; (2)
setting ``broker.web-socket.port`` on the command line or in the
configuration file; or (3) from C++ by calling
``endpoint::web_socket_listen()``. When running inside Zeek, scripts
may call ``Broker::listen_websocket()`` to have Zeek start listening
for incoming WebSocket connections.

.. note::

  Broker uses the same SSL parameters for native and WebSocket peers.

JSON API v1
-----------

To access the JSON API, clients may connect to
``wss://<host>:<port>/v1/messages/json`` (SSL enabled, default) or
``ws://<host>:<port>/v1/messages/json`` (SSL disabled). On this WebSocket
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

The WebSocket server may send `Data Messages`_ (whenever a data message matches
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

Represents a user-defined message with topic and data.

A data message consists of these keys:

``type``
  Always ``data-message``.

``topic``
  The Broker topic for the message. A client will only receive topics that match
  its subscriptions.

``@data-type``
  Meta field that encodes how to parse the ``data`` field (see
  `Data Representation`_).

``data``
  Contains the actual payload of the message.

Example:

.. code-block:: json

  {
    "type": "data-message",
    "topic": "/foo/bar",
    "@data-type": "count",
    "data": 1
  }

Error Messages
~~~~~~~~~~~~~~

The error messages on the WebSocket connection give feedback to the client if
the server discarded malformed input from the client or if there has been an
error while processing the JSON text.

An error message consists of these keys:

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

Encoding of Zeek Events
~~~~~~~~~~~~~~~~~~~~~~~

Broker encodes Zeek events as nested vectors using the following structure:
``[<format-nr>, <type>, [<name>, <args>, <metadata (optional)>]]``:

``format-nr``
  A ``count`` denoting the format version. Currently, this is always ``1``.

``type``
  A ``count`` denoting the encoded Zeek message type. For events, this is always
  ``1``. Other message types in Zeek are currently not safe for 3rd-party use.

``name``
  Identifies the Zeek event.

``args``
  Contains the arguments for the event in the form of another ``vector``.

``metadata``
  Contains a ``vector`` of key-value pairs (represented as further ``vectors``
  of size 2) for which the first element is a ``count`` for identification
  purposes and the second element any supported Broker data type. This vector
  can be used to attach arbitrary metadata to events.

  Zeek version 6.0 and up always includes the network time of an event as metadata.
  The key for a network timestamp is ``1`` and the data type for the value is
  a ``timestamp``.

  Broker endpoints are free to use counts starting with 200 to identify
  and exchange metadata of their own choosing. Within a network of Broker
  nodes, individual endpoints need to agree on the meaning and type of metadata
  attached to events.


For example, an event called ``event_1`` that has been published to topic
``/foo/bar`` with an integer argument ``42`` and a string argument ``test``
without attached metadata would be render as:

.. code-block:: json

  {
    "type": "data-message",
    "topic": "/foo/bar",
    "@data-type": "vector",
    "data": [
      {
        "@data-type": "count",
        "data": 1
      },
      {
        "@data-type": "count",
        "data": 1
      },
      {
        "@data-type": "vector",
        "data": [
          {
            "@data-type": "string",
            "data": "event_1"
          },
          {
            "@data-type": "vector",
            "data": [
              {
                "@data-type": "integer",
                "data": 42
              },
              {
                "@data-type": "string",
                "data": "test"
              }
            ]
          }
        ]
      }
    ]
  }

An event including with ``NetworkTimestamp`` metadata event render as follows,
having the ``args`` vector followed by another vector containing the network
timestamp of the event:

.. code-block:: json

  {
    "type": "data-message",
    "topic": "/foo/bar",
    "@data-type": "vector",
    "data": [
      {
        "@data-type": "count",
        "data": 1
      },
      {
        "@data-type": "count",
        "data": 1
      },
      {
        "@data-type": "vector",
        "data": [
          {
            "@data-type": "string",
            "data": "event_1"
          },
          {
            "@data-type": "vector",
            "data": [
              {
                "@data-type": "integer",
                "data": 42
              },
              {
                "@data-type": "string",
                "data": "test"
              }
            ]
          },
          {
            "@data-type": "vector",
            "data": [
              {
                "@data-type": "vector",
                "data": [
                  {
                    "@data-type": "count",
                    "data": 1
                  },
                  {
                    "@data-type": "timestamp",
                    "data": "2023-04-18T14:13:14.000"
                  }
                ]
              }
            ]
          }
        ]
      }
    ]
  }
