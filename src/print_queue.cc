#include "broker/print_queue.hh"
#include "broker/Endpoint.hh"
#include "print_queue_impl.hh"
#include <caf/scoped_actor.hpp>

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

static std::deque<std::string> request_msgs(const caf::actor& actor,
                                            caf::atom_value request_type)
	{
	std::deque<std::string> rval;
	caf::scoped_actor self;
	self->sync_send(actor, request_type).await(
		caf::on_arg_match >> [&rval](std::deque<std::string>& msgs)
			{ rval = std::move(msgs); }
	);
	return rval;
	}

std::deque<std::string> broker::print_queue::want_pop()
	{
	return request_msgs(pimpl->actor, caf::atom("want"));
	}

std::deque<std::string> broker::print_queue::need_pop()
	{
	return request_msgs(pimpl->actor, caf::atom("need"));
	}
