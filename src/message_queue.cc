#include "broker/message_queue.hh"
#include <caf/send.hpp>

class broker::message_queue::impl {
public:

	broker::topic subscription_prefix;
};

broker::message_queue::message_queue() = default;

broker::message_queue::~message_queue() = default;

broker::message_queue::message_queue(message_queue&&) = default;

broker::message_queue&
broker::message_queue::operator=(message_queue&&) = default;

broker::message_queue::message_queue(topic prefix, const endpoint& e)
	: broker::queue<broker::message>(),
      pimpl(new impl{std::move(prefix)})
	{
	caf::anon_send(*static_cast<caf::actor*>(e.handle()),
	               caf::atom("local sub"), pimpl->subscription_prefix,
	               *static_cast<caf::actor*>(this->handle()));
	}

const broker::topic& broker::message_queue::get_topic_prefix() const
	{ return pimpl->subscription_prefix; }

broker::message_queue::operator bool() const
	{ return pimpl != nullptr; }
