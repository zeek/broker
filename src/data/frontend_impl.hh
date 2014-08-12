#ifndef BROKER_DATA_FRONTEND_IMPL_HH
#define BROKER_DATA_FRONTEND_IMPL_HH

#include "broker/data/frontend.hh"
#include "../subscription.hh"
#include <caf/actor.hpp>

namespace broker { namespace data {

class frontend::impl {
public:

	impl(std::string t, caf::actor e, subscription rt, subscription dt)
		: topic(std::move(t)), endpoint(std::move(e)),
	      request_topic(std::move(rt)), data_topic(std::move(dt))
		{ }

	std::string topic;
	caf::actor endpoint;
	subscription request_topic;
	subscription data_topic;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_FRONTEND_IMPL_HH
