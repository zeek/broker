Formatting
==========

Broker uses C++20's ``std::format`` library for string formatting throughout the
codebase and provides formatters for its own types.

String Conversion
~~~~~~~~~~~~~~~~~

All types in Broker support formatting through the `convert` function pattern:

.. code-block:: cpp

   void convert(const T& value, std::string& output);
   std::string to_string(const T& value);

The `convert` function is the primary formatting function that writes to an
output string. The `to_string` function is a convenience wrapper that calls
`convert`.

Any type in Broker that provides a ``convert`` function can be formatted with
``std::format``. The formatter implementations are defined in
``broker/format.hh``. Only after including this header, Broker types can be
formatted with ``std::format``.

Basic Formatting
~~~~~~~~~~~~~~~

.. code-block:: cpp

   #include "broker/data.hh"
   #include "broker/format.hh"

   auto value = broker::data{42};
   auto result = std::format("Value: {}", value);

Logging
~~~~~~~

The logging system uses ``std::format`` internally. In addition to the format
string and the arguments, all logging functions take an event name as first
argument. This name is used to identify the event in the logging system and can
be used to filter events.

For example, the following code logs a message with severity ``info`` and the
event name ``my-event-name`` for the ``core`` component:

.. code-block:: cpp

   broker::log::core::info("my-event-name", "Processing message: {}", message);

Available severity levels are:

- ``critical``: fatal errors that prevent the application from continuing
- ``error``: severe events that most likely result in a loss of functionality
- ``warning``: events that indicate a potential problem
- ``info``: informational messages that provide context to the user
- ``verbose``: detailed messages that provide additional context
- ``debug``: messages that provide detailed information for debugging

Available components are:

- ``core``: logging for core Broker components
- ``endpoint``: logging related to the local endpoint
- ``store``: logging related to data stores
- ``network``: logging related to network components
- ``app``: logging from the application layer (not used by Broker itself)

The name of the component combined with the severity level forms the full
function name. For example, the function name for the ``core`` component and
``info`` severity level is ``broker::log::core::info``.
