#ifndef BROKER_UTIL_QUEUE_HH
#define BROKER_UTIL_QUEUE_HH

#include "flare.hh"
#include <caf/sb_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <deque>

namespace broker {
namespace util {

template <typename Pattern, typename Message>
class queue : public caf::sb_actor<queue<Pattern, Message>> {
friend class caf::sb_actor<queue<Pattern, Message>>;

public:

	queue(flare f)
		: ready_flare(std::move(f))
		{
		using namespace caf;
		message_handler common
			{
			on(atom("want")) >> [=]
				{ return pop(); },
			Pattern() >> [=](Message& msg)
				{
				q.push_back(std::move(msg));
				this->become(filled);
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
	caf::behavior& init_state = empty;
	std::deque<Message> q;
};

template <typename Message>
std::deque<Message>
queue_pop(const caf::actor& actor, caf::atom_value request_type)
	{
	std::deque<Message> rval;
	caf::scoped_actor self;
	self->sync_send(actor, request_type).await(
		caf::on_arg_match >> [&rval](std::deque<Message>& msgs)
			{
			rval = std::move(msgs);
			}
	);
	return rval;
	}

} // namespace util
} // namespace broker

#endif // BROKER_UTIL_QUEUE_HH
