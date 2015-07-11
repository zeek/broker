#ifndef BROKER_UTIL_QUEUE_ACTOR_HH
#define BROKER_UTIL_QUEUE_ACTOR_HH

#include "../atoms.hh"
#include "flare.hh"
#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <deque>

namespace broker {
namespace util {

/**
 * Provides a generic queuing mechanism implemented as an actor combined with
 * a flare which signals when the queue is non-empty.  This makes it simple
 * to integrate in to traditional event loops.
 */
template <typename Message>
class queue_actor : public caf::event_based_actor {

public:

	queue_actor(flare f)
		: ready_flare(std::move(f))
		{
		using namespace caf;
		message_handler common
			{
			[=](want_atom)
				{ return pop(); },
			[=](Message& msg)
				{
				q.push_back(std::move(msg));
				this->become(filled);
				ready_flare.fire();
				}
			};

		empty = common;
		filled = common.or_else(
			[=](need_atom)
				{ return pop(); }
		);
		}

private:

	caf::behavior make_behavior() override
		{
		return empty;
		}

	std::deque<Message> pop()
		{
		auto rval = std::move(q);
		q = {};
		this->become(empty);
		ready_flare.extinguish();
		return rval;
		}

	flare ready_flare;
	caf::behavior empty;
	caf::behavior filled;
	std::deque<Message> q;
};

template <typename Message>
std::deque<Message>
queue_pop(const caf::scoped_actor& self, const caf::actor& actor,
          caf::atom_value request_type)
	{
	std::deque<Message> rval;
	self->send(caf::message_priority::high, actor, request_type);
	self->receive(
		[&rval](std::deque<Message>& msgs)
			{
			rval = std::move(msgs);
			}
	);
	return rval;
	}

} // namespace util
} // namespace broker

#endif // BROKER_UTIL_QUEUE_ACTOR_HH
