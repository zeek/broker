.. _data-model:

Data Model
==========

Arithmetic
----------

Boolean
~~~~~~~

Count
~~~~~

Integer
~~~~~~~

Real
~~~~

Time
----

Broker offers two data types for expressing time: ``time::duration`` and
``time::point``. A duration represents relative time span, whereas as a point
an absolute point in time. Time durations have nanosecond granularity and time
points. Instances of ``time::point`` are anchored at the UNIX epoch, January 1,
1970. The function ``time::now()`` returns the current wallclock time as a
``time::point``.

Both types seemlessly interoperate with the C++ standard library time
faciliates. In fact, they are instances of ``std::chrono::duration`` and
``std::chrono::time_point``::

  namespace broker {
  namespace time {

  using clock = std::chrono::system_clock;
  using duration = std::chrono::duration<int64_t, std::nano>;
  using point = std::chrono::time_point<clock, duration>;

  point now();

  } // namespace time
  } // namespace broker

String
------

Networking
----------

Address
~~~~~~~

Subnet
~~~~~~

Port
~~~~

Containers
----------

Vector
~~~~~~

Set
~~~

Table
~~~~~
