#ifndef BROKER_LOG_QUEUE_IMPL_HH
#define BROKER_LOG_QUEUE_IMPL_HH

#include "broker/log_queue.hh"
#include "broker/topic.hh"
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
		topic_name = std::move(t);
		actor = caf::spawn<queue<decltype(caf::on<topic, int, log_msg>()),
		                         log_msg>>(std::move(f));
		self->planned_exit_reason(caf::exit_reason::user_defined);
		actor->link_to(self);

		caf::anon_send(*static_cast<caf::actor*>(e.handle()),
		               caf::atom("local sub"),
		               topic{topic_name, topic::tag::log}, actor);
		}

	int fd;
	std::string topic_name;
	caf::scoped_actor self;
	caf::actor actor;
};

} // namespace broker

#endif // BROKER_LOG_QUEUE_IMPL_HH
