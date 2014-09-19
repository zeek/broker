#include "response_queue_impl.hh"

broker::store::response_queue::response_queue()
	: pimpl(new impl)
	{
	}

broker::store::response_queue::~response_queue() = default;

broker::store::response_queue::response_queue(response_queue&& other) = default;

broker::store::response_queue&
broker::store::response_queue::operator=(response_queue&& other) = default;

int broker::store::response_queue::fd() const
	{
	return pimpl->fd;
	}

void* broker::store::response_queue::handle() const
	{
	return &pimpl->actor;
	}

std::deque<broker::store::response>
broker::store::response_queue::want_pop() const
	{
	return queue_pop<response>(pimpl->actor, caf::atom("want"));
	}

std::deque<broker::store::response>
broker::store::response_queue::need_pop() const
	{
	return queue_pop<response>(pimpl->actor, caf::atom("need"));
	}
