#include "print_queue_impl.hh"
#include "broker/Endpoint.hh"

broker::print_queue::print_queue()
    : pimpl(std::make_shared<impl>())
	{
	}

broker::print_queue::print_queue(std::string topic, const endpoint& e)
    : pimpl(std::make_shared<impl>(std::move(topic), e))
	{
	}

int broker::print_queue::fd() const
	{
	return pimpl->ready_flare.fd();
	}

const std::string& broker::print_queue::topic() const
	{
	return pimpl->topic;
	}

std::deque<std::string> broker::print_queue::want_pop()
	{
	return queue_pop<std::string>(pimpl->actor, caf::atom("want"));
	}

std::deque<std::string> broker::print_queue::need_pop()
	{
	return queue_pop<std::string>(pimpl->actor, caf::atom("need"));
	}
