#ifndef BROKER_LOG_QUEUE_IMPL_HH
#define BROKER_LOG_QUEUE_IMPL_HH

#include "broker/log_queue.hh"
#include "subscription.hh"
#include "util/flare.hh"
#include "util/queue.hh"
#include <caf/spawn.hpp>
#include <caf/send.hpp>

namespace broker {

class log_queue::impl {
public:

	impl() = default;

	impl(std::string t, const endpoint& e)
		{
		using broker::util::queue;
		util::flare f;
		fd = f.fd();
		topic = std::move(t);
		actor = caf::spawn<queue<decltype(caf::on<subscription, log_msg>()),
		                         log_msg>>(std::move(f));
		self->planned_exit_reason(caf::exit_reason::user_defined);
		actor->link_to(self);

		caf::anon_send(*static_cast<caf::actor*>(e.handle()), caf::atom("sub"),
		               subscription{subscription_type::log, topic}, actor);
		}

	int fd;
	std::string topic;
	caf::scoped_actor self;
	caf::actor actor;
};

} // namespace broker

#endif // BROKER_LOG_QUEUE_IMPL_HH
