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

broker::print_queue::print_queue(std::string topic_name, const endpoint& e)
    : pimpl(new impl(std::move(topic_name), e))
	{
	}

int broker::print_queue::fd() const
	{
	return pimpl->fd;
	}

const std::string& broker::print_queue::topic_name() const
	{
	return pimpl->topic_name;
	}

std::deque<broker::print_msg> broker::print_queue::want_pop() const
	{
	return util::queue_pop<print_msg>(pimpl->actor, caf::atom("want"));
	}

std::deque<broker::print_msg> broker::print_queue::need_pop() const
	{
	return util::queue_pop<print_msg>(pimpl->actor, caf::atom("need"));
	}
