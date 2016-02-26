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

	impl(caf::actor ea, caf::actor pa, bool r = false)
		: endpoint_actor(std::move(ea)), peer_actor(std::move(pa)),
		  remote(r), remote_tuple(get_remote_tuple(pa))
		{ }

	bool operator==(const peering::impl& other) const;

	caf::actor endpoint_actor;
	caf::actor peer_actor;
	bool remote;
	std::pair<std::string, uint16_t> remote_tuple;

private:
	std::pair<std::string, uint16_t> get_remote_tuple(const caf::actor& p) const;
};

inline std::pair<std::string, uint16_t> peering::impl::get_remote_tuple(const caf::actor& p) const
	{
	// TODO: Fill in via CAF.
	std::string addr = "127.0.0.1";
	uint16_t port = 9999;
	return std::make_pair(addr, port);
	}

inline bool peering::impl::operator==(const peering::impl& other) const
	{
	return endpoint_actor == other.endpoint_actor &&
	       peer_actor == other.peer_actor &&
	       remote == other.remote;
	}

} // namespace broker

#endif // BROKER_PEERING_IMPL_HH
