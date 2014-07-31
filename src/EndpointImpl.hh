#ifndef BROKER_ENDPOINTIMPL_HH
#define BROKER_ENDPOINTIMPL_HH

#include "broker/Endpoint.hh"

#include <caf/actor.hpp>

#include <unordered_map>
#include <string>

namespace broker {

class Endpoint::Impl {
public:

	std::string name;
	caf::actor endpoint;
	std::unordered_map<caf::actor, Peer> peers;
	int last_errno;
	std::string last_error;
};
} // namespace broker

#endif // BROKER_ENDPOINTIMPL_HH
