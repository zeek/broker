#ifndef BROKER_NONBLOCKING_ENDPOINT_HH
#define BROKER_NONBLOCKING_ENDPOINT_HH

#include "broker/endpoint.hh"

namespace broker {

/// An endpoint with an asynchronous (nonblocking) messaging API.
class nonblocking_endpoint : public endpoint {
  friend context; // construction

  nonblocking_endpoint(caf::actor_system& sys, caf::behavior bhvr);
};

} // namespace broker

#endif // BROKER_NONBLOCKING_ENDPOINT_HH
