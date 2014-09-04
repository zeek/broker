#include "print_queue_impl.hh"
#include "broker/Endpoint.hh"

broker::print_queue::print_queue()
    : pimpl(new impl)
	{
	}

broker::print_queue::~print_queue() = default;

broker::print_queue::print_queue(print_queue&& other) = default;

broker::print_queue&
broker::print_queue::operator=(print_queue&& other) = default;

broker::print_queue::print_queue(std::string topic, const endpoint& e)
    : pimpl(new impl(std::move(topic), e))
	{
	}

int broker::print_queue::fd() const
	{
	return pimpl->fd;
	}

const std::string& broker::print_queue::topic() const
	{
	return pimpl->topic;
	}

std::deque<broker::print_msg> broker::print_queue::want_pop() const
	{
	return queue_pop<print_msg>(pimpl->actor, caf::atom("want"));
	}

std::deque<broker::print_msg> broker::print_queue::need_pop() const
	{
	return queue_pop<print_msg>(pimpl->actor, caf::atom("need"));
	}
