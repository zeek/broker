#ifndef BROKER_DATA_RESPONSE_QUEUE_IMPL_HH
#define BROKER_DATA_RESPONSE_QUEUE_IMPL_HH

#include "broker/data/response_queue.hh"
#include "../util/flare.hh"
#include "../util/queue.hh"
#include <caf/spawn.hpp>
#include <caf/send.hpp>

namespace broker { namespace data {

class response_queue::impl {
public:

	impl()
		: ready_flare(),
	      actor(caf::spawn<broker::queue<decltype(caf::on<response>()),
	                                     response>>(ready_flare))
		{ }

	~impl()
		{
		caf::anon_send(actor, caf::atom("quit"));
		}

	const flare ready_flare;
	caf::actor actor;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_RESPONSE_QUEUE_IMPL_HH
