===============================
Broker: Bro's Messaging Library
===============================

The Broker library implements Bro's high-level communication patterns:

    - remote logging
    - remote printing
    - remote events
    - distributed data stores

Logging, printing, and events all follow a pub/sub communication model
between Broker endpoints that are directly peered with each other.  An
endpoint also has the option of subscribing to its own messages.
Subscriptions are matched prefix-wise and endpoints have the capability
of fine-tuning the subscription topic strings they wish to advertise to
peers as well as the messages they wish to send to them.

The distributed data store functionality allows a master data store
associated with one Broker endpoint to be cloned at peer endpoints which
may then perform lightweight, local queries against the clone, which
automatically stays synchronized with the master store.  Clones
cannot modify their content directly, instead they send modifications
to the centralized master store which applies them and then broadcasts
them to all clones.

Applications which integrate the Broker library may communicate with
each other using the above-mentioned patterns which are common to Bro.

Dependencies
------------

Compiling Broker requires the following libraries/tools to already be
installed:

    A C++11 capable compiler (GCC 4.8+ or Clang 3.3+)

    CAF (C++ Actor Framework) version 0.14+
        https://github.com/actor-framework/actor-framework

    CMake 2.8+
        CMake is a cross-platform, open-source build system, typically
        not installed by default.  See http://www.cmake.org for more
        information regarding CMake and the installation steps below
        for how to use it to build this distribution.  CMake generates
        native Makefiles that depend on GNU Make by default.

And optionally:

    SWIG 3.0.3+, http://www.swig.org/
        SWIG is used to optionally build Python bindings for Broker.

Compiling/Installing
--------------------

To compile and install in to ``/usr/local``::

    ./configure
    make
    make install

See ``./configure --help`` for more advanced configuration options.

Documentation
-------------

Please see the `Broker User Manual <./broker-manual.rst>`_ for more
documentation and examples of how to use the library.
