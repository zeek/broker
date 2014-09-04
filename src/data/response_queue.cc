#include "response_queue_impl.hh"

broker::data::response_queue::response_queue()
	: pimpl(new impl)
	{
	}

broker::data::response_queue::~response_queue() = default;

broker::data::response_queue::response_queue(response_queue&& other) = default;

broker::data::response_queue&
broker::data::response_queue::operator=(response_queue&& other) = default;

int broker::data::response_queue::fd() const
	{
	return pimpl->fd;
	}

void* broker::data::response_queue::handle() const
	{
	return &pimpl->actor;
	}

std::deque<broker::data::response>
broker::data::response_queue::want_pop() const
	{
	return queue_pop<response>(pimpl->actor, caf::atom("want"));
	}

std::deque<broker::data::response>
broker::data::response_queue::need_pop() const
	{
	return queue_pop<response>(pimpl->actor, caf::atom("need"));
	}
