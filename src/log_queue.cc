#include "log_queue_impl.hh"
#include "broker/Endpoint.hh"

broker::log_queue::log_queue()
    : pimpl(new impl)
	{
	}

broker::log_queue::~log_queue() = default;

broker::log_queue::log_queue(log_queue&& other) = default;

broker::log_queue&
broker::log_queue::operator=(log_queue&& other) = default;

broker::log_queue::log_queue(std::string topic, const endpoint& e)
    : pimpl(new impl(std::move(topic), e))
	{
	}

int broker::log_queue::fd() const
	{
	return pimpl->fd;
	}

const std::string& broker::log_queue::topic() const
	{
	return pimpl->topic;
	}

std::deque<broker::log_msg> broker::log_queue::want_pop() const
	{
	return util::queue_pop<log_msg>(pimpl->actor, caf::atom("want"));
	}

std::deque<broker::log_msg> broker::log_queue::need_pop() const
	{
	return util::queue_pop<log_msg>(pimpl->actor, caf::atom("need"));
	}
