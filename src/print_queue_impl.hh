#ifndef BROKER_PRINT_QUEUE_IMPL_HH
#define BROKER_PRINT_QUEUE_IMPL_HH

#include "broker/print_queue.hh"
#include "subscription.hh"
#include "flare.hh"
#include <caf/spawn.hpp>
#include <caf/send.hpp>
#include <caf/sb_actor.hpp>

namespace broker {

class print_queue_actor : public caf::sb_actor<print_queue_actor> {
friend class caf::sb_actor<print_queue_actor>;

public:

	print_queue_actor(flare f)
		: ready_flare(std::move(f))
		{
		using namespace caf;
		message_handler common
			{
			on(atom("quit")) >> [=]
				{ quit(); },
			on(atom("want")) >> [=]
				{ return pop(); },
			on<subscription, std::string>() >> [=](std::string& msg)
				{
				queue.push_back(std::move(msg));
				become(filled);
				ready_flare.fire();
				}
			};

		empty = common;
		filled = common.or_else(
			on(atom("need")) >> [=]
				{ return pop(); }
		);
		}

private:

	std::deque<std::string> pop()
		{
		auto rval = std::move(queue);
		queue = {};
		become(empty);
		ready_flare.extinguish();
		return rval;
		}

	flare ready_flare;
	caf::behavior empty;
	caf::behavior filled;
	caf::behavior& init_state = empty;
	std::deque<std::string> queue;
};

class print_queue::impl {
public:

	impl() = default;

	impl(std::string t, const endpoint& e)
		: ready_flare(), topic(std::move(t)),
	      actor(caf::spawn<broker::print_queue_actor>(ready_flare))
		{
		caf::anon_send(*static_cast<caf::actor*>(e.handle()), caf::atom("sub"),
		               subscription{subscription_type::print, topic}, actor);
		}

	~impl()
		{
		caf::anon_send(actor, caf::atom("quit"));
		}

	const flare ready_flare;
	std::string topic;
	caf::actor actor;
};

} // namespace broker

#endif // BROKER_PRINT_QUEUE_IMPL_HH
