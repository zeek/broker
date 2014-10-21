#ifndef BROKER_PRINT_QUEUE_IMPL_HH
#define BROKER_PRINT_QUEUE_IMPL_HH

#include "broker/print_queue.hh"
#include "broker/topic.hh"
#include "util/flare.hh"
#include "util/queue.hh"
#include <caf/spawn.hpp>
#include <caf/send.hpp>

namespace broker {

class print_queue::impl {
public:

	impl() = default;

	impl(std::string t, const endpoint& e)
		{
		using broker::util::queue;
		util::flare f;
		fd = f.fd();
		topic_name = std::move(t);
		actor = caf::spawn<queue<decltype(caf::on<topic, int, print_msg>()),
		                         print_msg>>(std::move(f));
		self->planned_exit_reason(caf::exit_reason::user_defined);
		actor->link_to(self);

		caf::anon_send(*static_cast<caf::actor*>(e.handle()),
		               caf::atom("local sub"),
		               topic{topic_name, topic::tag::print}, actor);
		}

	int fd;
	std::string topic_name;
	caf::scoped_actor self;
	caf::actor actor;
};

} // namespace broker

#endif // BROKER_PRINT_QUEUE_IMPL_HH
