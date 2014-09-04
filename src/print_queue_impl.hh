#ifndef BROKER_PRINT_QUEUE_IMPL_HH
#define BROKER_PRINT_QUEUE_IMPL_HH

#include "broker/print_queue.hh"
#include "subscription.hh"
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
		flare f;
		fd = f.fd();
		topic = std::move(t);
		using std::string;
		using qt = queue<decltype(caf::on<subscription, string>()), string>;
		actor = caf::spawn<qt>(std::move(f));

		caf::anon_send(*static_cast<caf::actor*>(e.handle()), caf::atom("sub"),
		               subscription{subscription_type::print, topic}, actor);
		}

	~impl()
		{
		caf::anon_send(actor, caf::atom("quit"));
		}

	int fd;
	std::string topic;
	caf::actor actor;
};

} // namespace broker

#endif // BROKER_PRINT_QUEUE_IMPL_HH
