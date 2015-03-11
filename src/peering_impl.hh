#ifndef BROKER_PEERING_IMPL_HH
#define BROKER_PEERING_IMPL_HH

#include "broker/peering.hh"
#include <caf/actor.hpp>
#include <utility>
#include <string>
#include <cstdint>

namespace broker {

class peering::impl {
public:

	impl()
		: endpoint_actor(), peer_actor(), remote(), remote_tuple()
		{ }

	impl(caf::actor ea, caf::actor pa, bool r = false,
	     std::pair<std::string, uint16_t> rt = std::make_pair("", 0))
		: endpoint_actor(std::move(ea)), peer_actor(std::move(pa)),
		  remote(r), remote_tuple(std::move(rt))
		{ }

	bool operator==(const peering::impl& other) const;

	caf::actor endpoint_actor;
	caf::actor peer_actor;
	bool remote;
	std::pair<std::string, uint16_t> remote_tuple;
};

inline bool peering::impl::operator==(const peering::impl& other) const
	{
	return endpoint_actor == other.endpoint_actor &&
	       peer_actor == other.peer_actor &&
	       remote == other.remote &&
	       remote_tuple == other.remote_tuple;
	}

} // namespace broker

#endif // BROKER_PEERING_IMPL_HH
