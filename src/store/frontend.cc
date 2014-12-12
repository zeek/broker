#include "frontend_impl.hh"
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <caf/sb_actor.hpp>
#include <caf/spawn.hpp>

static inline caf::actor& handle_to_actor(void* h)
	{
	return *static_cast<caf::actor*>(h);
	}

broker::store::frontend::frontend(const endpoint& e, identifier master_name)
    : pimpl(new impl(std::move(master_name), handle_to_actor(e.handle())))
	{
	}

broker::store::frontend::~frontend() = default;

broker::store::frontend::frontend(frontend&& other) = default;

broker::store::frontend&
broker::store::frontend::operator=(frontend&& other) = default;

const broker::store::identifier& broker::store::frontend::id() const
	{
	return pimpl->master_name;
	}

const broker::store::response_queue& broker::store::frontend::responses() const
	{
	return pimpl->responses;
	}

void broker::store::frontend::insert(data k, data v,
                                     util::optional<expiration_time> t) const
	{
	if ( t )
		caf::anon_send(handle_to_actor(handle()),
		               pimpl->master_name, caf::atom("insert"),
		               std::move(k), std::move(v), *t);
	else
		caf::anon_send(handle_to_actor(handle()),
		               pimpl->master_name, caf::atom("insert"),
		               std::move(k), std::move(v));
	}

void broker::store::frontend::erase(data k) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, caf::atom("erase"),
	               std::move(k));
	}

void broker::store::frontend::clear() const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, caf::atom("clear"));
	}

void broker::store::frontend::increment(data k, int64_t by) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, caf::atom("increment"),
	               std::move(k), by);
	}

void broker::store::frontend::decrement(data k, int64_t by) const
	{
	increment(std::move(k), -by);
	}

void broker::store::frontend::add_to_set(data k, data element) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, caf::atom("set_add"),
	               std::move(k), std::move(element));
	}

void broker::store::frontend::remove_from_set(data k, data element) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, caf::atom("set_rem"),
	               std::move(k), std::move(element));
	}

void broker::store::frontend::push_left(data k, vector item) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, caf::atom("lpush"),
	               std::move(k), std::move(item));
	}

void broker::store::frontend::push_right(data k, vector item) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, caf::atom("rpush"),
	               std::move(k), std::move(item));
	}

broker::store::result broker::store::frontend::request(query q) const
	{
	result rval;
	caf::scoped_actor self;
	caf::actor store_actor = caf::invalid_actor;
	bool need_master = q.type == query::tag::pop_left ||
	                   q.type == query::tag::pop_right;
	caf::actor& where = need_master ? pimpl->endpoint
	                                : handle_to_actor(handle());

	self->sync_send(where, caf::atom("storeactor"), pimpl->master_name).await(
		caf::on_arg_match >> [&store_actor](caf::actor& sa)
			{
			store_actor = std::move(sa);
			}
	);

	if ( ! store_actor )
		return rval;

	self->sync_send(store_actor, pimpl->master_name, std::move(q), self).await(
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
	bool need_master = q.type == query::tag::pop_left ||
	                   q.type == query::tag::pop_right;
	caf::actor& where = need_master ? pimpl->endpoint
	                                : handle_to_actor(handle());

	caf::spawn<requester>(where,
	           pimpl->master_name, std::move(q),
	           handle_to_actor(pimpl->responses.handle()),
	           timeout, cookie);
	}

void* broker::store::frontend::handle() const
	{
	return &pimpl->endpoint;
	}
