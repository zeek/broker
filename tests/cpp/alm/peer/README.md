The `alm::peer` is the main component for implementing the core actor (which
ultimately drives the `broker::endpoint`). The class itself manages routing
information and subscriptions.

For communicating to peers in the network, the `peer` relies on a transport
layer. This layer is implemented by the derived class and the `peer` then uses
CRTP for calling the required functions. Please refer to the Developer Guide in
the manual for further details.

This design implies that we cannot test the `peer` class on its own. Neither can
we test the transport classes on their own, at least not without excessive
mocking.

For covering most basic functionality of `alm::peer` without the full CAF
streaming setup, we've split the unit tests for the `peer` into two test suites:

1. The first setup uses the `alm::async_transport`, which is basically a mockup
   transport that omits much functionality Broker requires in an actual
   deployment. This setup only covers the most basic operations of `alm::peer`
   for setting up a Broker cluster.
2. The second setup uses the `alm::stream_transport`, which drives the actual
   core actor in Broker. This test suite covers data transmission, path
   revocation, etc.
