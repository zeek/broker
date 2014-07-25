#ifndef BROKER_DATA_FACADEIMPL_HH
#define BROKER_DATA_FACADEIMPL_HH

#include "broker/data/Facade.hh"
#include "../Subscription.hh"

#include <cppa/cppa.hpp>

namespace broker { namespace data {

class Facade::Impl {
public:

	std::string topic;
	cppa::actor endpoint;
	SubscriptionTopic request_topic;
	SubscriptionTopic data_topic;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_FACADEIMPL_HH
