#ifndef BROKER_PEERIMPL_HH
#define BROKER_PEERIMPL_HH

#include "broker/Peer.hh"

#include <caf/actor.hpp>

#include <utility>
#include <string>
#include <cstdint>

namespace broker {

class Peer::Impl {
public:

	caf::actor endpoint = caf::invalid_actor;
	bool remote = false;
	std::pair<std::string, uint16_t> remote_tuple;
};

} // namespace broker

#endif // BROKER_PEERIMPL_HH
