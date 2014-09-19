#ifndef BROKER_STORE_RESPONSE_QUEUE_IMPL_HH
#define BROKER_STORE_RESPONSE_QUEUE_IMPL_HH

#include "broker/store/response_queue.hh"
#include "../util/flare.hh"
#include "../util/queue.hh"
#include <caf/spawn.hpp>
#include <caf/send.hpp>

namespace broker { namespace store {

class response_queue::impl {
public:

	impl()
		{
		flare f;
		fd = f.fd();
		actor = caf::spawn<broker::queue<decltype(caf::on<response>()),
		                                 response>>(std::move(f));
		self->planned_exit_reason(caf::exit_reason::user_defined);
		actor->link_to(self);
		}

	int fd;
	caf::scoped_actor self;
	caf::actor actor;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_RESPONSE_QUEUE_IMPL_HH
