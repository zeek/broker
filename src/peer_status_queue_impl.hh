#ifndef BROKER_PEER_STATUS_QUEUE_IMPL_HH
#define BROKER_PEER_STATUS_QUEUE_IMPL_HH

#include "broker/peer_status_queue.hh"
#include "util/flare.hh"
#include "util/queue.hh"
#include <caf/spawn.hpp>
#include <caf/send.hpp>

namespace broker {

class peer_status_queue::impl {
public:

	impl()
		{
		using broker::util::queue;
		util::flare f;
		fd = f.fd();
		actor = caf::spawn<queue<decltype(caf::on<peer_status>()),
		                         peer_status>>(std::move(f));
		self->planned_exit_reason(caf::exit_reason::user_defined);
		actor->link_to(self);
		}

	int fd;
	caf::scoped_actor self;
	caf::actor actor;
};

} // namespace broker

#endif // BROKER_PEER_STATUS_QUEUE_IMPL_HH
