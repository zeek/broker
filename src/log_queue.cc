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

broker::log_queue::log_queue(std::string topic_name, const endpoint& e)
    : pimpl(new impl(std::move(topic_name), e))
	{
	}

int broker::log_queue::fd() const
	{
	return pimpl->fd;
	}

const std::string& broker::log_queue::topic_name() const
	{
	return pimpl->topic_name;
	}

std::deque<broker::log_msg> broker::log_queue::want_pop() const
	{
	return util::queue_pop<log_msg>(pimpl->actor, caf::atom("want"));
	}

std::deque<broker::log_msg> broker::log_queue::need_pop() const
	{
	return util::queue_pop<log_msg>(pimpl->actor, caf::atom("need"));
	}
