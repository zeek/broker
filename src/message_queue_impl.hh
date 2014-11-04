#ifndef BROKER_MESSAGE_QUEUE_IMPL_HH
#define BROKER_MESSAGE_QUEUE_IMPL_HH

#include "broker/message_queue.hh"
#include "broker/topic.hh"
#include "util/flare.hh"
#include "util/queue.hh"
#include <caf/spawn.hpp>
#include <caf/send.hpp>

namespace broker {

class message_queue::impl {
public:

	impl() = default;

	impl(topic t, const endpoint& e)
		{
		using broker::util::queue;
		util::flare f;
		fd = f.fd();
		subscription = std::move(t);
		actor = caf::spawn<queue<decltype(caf::on<message>()),
		                         message>>(std::move(f));
		self->planned_exit_reason(caf::exit_reason::user_defined);
		actor->link_to(self);

		caf::anon_send(*static_cast<caf::actor*>(e.handle()),
		               caf::atom("local sub"), subscription, actor);
		}

	int fd;
	topic subscription;
	caf::scoped_actor self;
	caf::actor actor;
};

} // namespace broker

#endif // BROKER_MESSAGE_QUEUE_IMPL_HH
