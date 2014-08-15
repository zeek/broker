#include "response_queue_impl.hh"

broker::data::response_queue::response_queue()
	: pimpl(std::make_shared<impl>())
	{
	}

int broker::data::response_queue::fd() const
	{
	return pimpl->ready_flare.fd();
	}

void* broker::data::response_queue::handle() const
	{
	return &pimpl->actor;
	}

std::deque<broker::data::response> broker::data::response_queue::want_pop()
	{
	return queue_pop<response>(pimpl->actor, caf::atom("want"));
	}

std::deque<broker::data::response> broker::data::response_queue::need_pop()
	{
	return queue_pop<response>(pimpl->actor, caf::atom("need"));
	}
