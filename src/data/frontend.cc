#include "frontend_impl.hh"
#include <broker/data/store.hh>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <caf/sb_actor.hpp>
#include <caf/spawn.hpp>

static inline caf::actor& handle_to_actor(void* h)
	{
	return *static_cast<caf::actor*>(h);
	}

broker::data::frontend::frontend(const endpoint& e, std::string topic)
    : pimpl(std::make_shared<impl>(std::move(topic),
                                   handle_to_actor(e.handle())))
	{
	}

broker::data::frontend::~frontend() = default;

const std::string& broker::data::frontend::topic() const
	{
	return pimpl->topic;
	}

broker::data::response_queue broker::data::frontend::responses() const
	{
	return pimpl->responses;
	}

void broker::data::frontend::insert(key k, value v) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->data_topic, caf::atom("insert"),
	               std::move(k), std::move(v));
	}

void broker::data::frontend::erase(key k) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->data_topic, caf::atom("erase"),
	               std::move(k));
	}

void broker::data::frontend::clear() const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->data_topic, caf::atom("clear"));
	}

broker::data::result broker::data::frontend::request(query q) const
	{
	result rval;
	caf::scoped_actor self;
	self->send(handle_to_actor(handle()), pimpl->request_topic, std::move(q),
	           self);
	self->receive(
		caf::on_arg_match >> [&rval](const caf::actor&, result& r)
			{
			rval = std::move(r);
			}
	);
	return rval;
	}

void broker::data::frontend::request(query q,
                                     std::chrono::duration<double> timeout,
                                     void* cookie) const
	{
	caf::spawn<requester>(handle_to_actor(handle()),
	           pimpl->request_topic, std::move(q),
	           handle_to_actor(pimpl->responses.handle()),
	           timeout, cookie);
	}

void* broker::data::frontend::handle() const
	{
	return &pimpl->endpoint;
	}
