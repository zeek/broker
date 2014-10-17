#include "event_queue_impl.hh"
#include "broker/Endpoint.hh"

broker::event_queue::event_queue()
    : pimpl(new impl)
	{
	}

broker::event_queue::~event_queue() = default;

broker::event_queue::event_queue(event_queue&& other) = default;

broker::event_queue&
broker::event_queue::operator=(event_queue&& other) = default;

broker::event_queue::event_queue(std::string topic_name, const endpoint& e)
    : pimpl(new impl(std::move(topic_name), e))
	{
	}

int broker::event_queue::fd() const
	{
	return pimpl->fd;
	}

const std::string& broker::event_queue::topic_name() const
	{
	return pimpl->topic_name;
	}

std::deque<broker::event_msg> broker::event_queue::want_pop() const
	{
	return util::queue_pop<event_msg>(pimpl->actor, caf::atom("want"));
	}

std::deque<broker::event_msg> broker::event_queue::need_pop() const
	{
	return util::queue_pop<event_msg>(pimpl->actor, caf::atom("need"));
	}
