#include "frontend_impl.hh"
#include <broker/store/store.hh>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <caf/sb_actor.hpp>
#include <caf/spawn.hpp>

static inline caf::actor& handle_to_actor(void* h)
	{
	return *static_cast<caf::actor*>(h);
	}

broker::store::frontend::frontend(const endpoint& e, std::string topic_name)
    : pimpl(new impl(std::move(topic_name), handle_to_actor(e.handle())))
	{
	}

broker::store::frontend::~frontend() = default;

broker::store::frontend::frontend(frontend&& other) = default;

broker::store::frontend&
broker::store::frontend::operator=(frontend&& other) = default;

const std::string& broker::store::frontend::topic_name() const
	{
	return pimpl->topic_name;
	}

const broker::store::response_queue& broker::store::frontend::responses() const
	{
	return pimpl->responses;
	}

void broker::store::frontend::insert(data k, data v) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->update_topic, caf::atom("insert"),
	               std::move(k), std::move(v));
	}

void broker::store::frontend::erase(data k) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->update_topic, caf::atom("erase"),
	               std::move(k));
	}

void broker::store::frontend::clear() const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->update_topic, caf::atom("clear"));
	}

broker::store::result broker::store::frontend::request(query q) const
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

void broker::store::frontend::request(query q,
                                      std::chrono::duration<double> timeout,
                                      void* cookie) const
	{
	caf::spawn<requester>(handle_to_actor(handle()),
	           pimpl->request_topic, std::move(q),
	           handle_to_actor(pimpl->responses.handle()),
	           timeout, cookie);
	}

void* broker::store::frontend::handle() const
	{
	return &pimpl->endpoint;
	}
