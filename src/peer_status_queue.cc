#include "peer_status_queue_impl.hh"

broker::peer_status_queue::peer_status_queue()
	: pimpl(new impl)
	{
	}

broker::peer_status_queue::~peer_status_queue() = default;

broker::peer_status_queue::peer_status_queue(peer_status_queue&& other) = default;

broker::peer_status_queue&
broker::peer_status_queue::operator=(peer_status_queue&& other) = default;

int broker::peer_status_queue::fd() const
	{
	return pimpl->fd;
	}

void* broker::peer_status_queue::handle() const
	{
	return &pimpl->actor;
	}

std::deque<broker::peer_status>
broker::peer_status_queue::want_pop() const
	{
	return util::queue_pop<peer_status>(pimpl->actor, caf::atom("want"));
	}

std::deque<broker::peer_status>
broker::peer_status_queue::need_pop() const
	{
	return util::queue_pop<peer_status>(pimpl->actor, caf::atom("need"));
	}
