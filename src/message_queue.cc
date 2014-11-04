#include "message_queue_impl.hh"
#include "broker/endpoint.hh"

broker::message_queue::message_queue()
    : pimpl(new impl)
	{
	}

broker::message_queue::~message_queue() = default;

broker::message_queue::message_queue(message_queue&& other) = default;

broker::message_queue&
broker::message_queue::operator=(message_queue&& other) = default;

broker::message_queue::message_queue(topic t, const endpoint& e)
    : pimpl(new impl(std::move(t), e))
	{
	}

int broker::message_queue::fd() const
	{
	return pimpl->fd;
	}

const broker::topic& broker::message_queue::get_topic() const
	{
	return pimpl->subscription;
	}

std::deque<broker::message> broker::message_queue::want_pop() const
	{
	return util::queue_pop<message>(pimpl->actor, caf::atom("want"));
	}

std::deque<broker::message> broker::message_queue::need_pop() const
	{
	return util::queue_pop<message>(pimpl->actor, caf::atom("need"));
	}
