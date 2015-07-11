#include "frontend_impl.hh"
#include "../atoms.hh"
#include <caf/send.hpp>
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

void broker::store::frontend::insert(data k, data v) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, insert_atom::value,
	               std::move(k), std::move(v));
	}

void broker::store::frontend::insert(data k, data v, expiration_time t) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, insert_atom::value,
	               std::move(k), std::move(v), std::move(t));
	}

void broker::store::frontend::erase(data k) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, erase_atom::value,
	               std::move(k));
	}

void broker::store::frontend::clear() const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, clear_atom::value);
	}

void broker::store::frontend::increment(data k, int64_t by) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, increment_atom::value,
	               std::move(k), by);
	}

void broker::store::frontend::decrement(data k, int64_t by) const
	{
	increment(std::move(k), -by);
	}

void broker::store::frontend::add_to_set(data k, data element) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, set_add_atom::value,
	               std::move(k), std::move(element));
	}

void broker::store::frontend::remove_from_set(data k, data element) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, set_rem_atom::value,
	               std::move(k), std::move(element));
	}

void broker::store::frontend::push_left(data k, vector item) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, lpush_atom::value,
	               std::move(k), std::move(item));
	}

void broker::store::frontend::push_right(data k, vector item) const
	{
	caf::anon_send(handle_to_actor(handle()),
	               pimpl->master_name, rpush_atom::value,
	               std::move(k), std::move(item));
	}

broker::store::result broker::store::frontend::request(query q) const
	{
	result rval;
	caf::actor store_actor = caf::invalid_actor;
	bool need_master = q.type == query::tag::pop_left ||
	                   q.type == query::tag::pop_right;
	caf::actor& where = need_master ? pimpl->endpoint
	                                : handle_to_actor(handle());

	pimpl->self->sync_send(where, store_actor_atom::value,
	                       pimpl->master_name).await(
		[&store_actor](caf::actor& sa)
			{
			store_actor = std::move(sa);
			}
	);

	if ( ! store_actor )
		return rval;

	pimpl->self->sync_send(store_actor, pimpl->master_name,
	                       std::move(q), pimpl->self).await(
		[&rval](const caf::actor&, result& r)
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

// Begin C API
#include "broker/broker.h"
using std::nothrow;

void broker_deque_of_store_response_delete(broker_deque_of_store_response* d)
	{
	delete reinterpret_cast<std::deque<broker::store::response>*>(d);
	}

size_t
broker_deque_of_store_response_size(const broker_deque_of_store_response* d)
	{
	auto dd = reinterpret_cast<const std::deque<broker::store::response>*>(d);
	return dd->size();
	}

broker_store_response*
broker_deque_of_store_response_at(broker_deque_of_store_response* d,
                                  size_t idx)
	{
	auto dd = reinterpret_cast<std::deque<broker::store::response>*>(d);
	return reinterpret_cast<broker_store_response*>(&(*dd)[idx]);
	}

void broker_deque_of_store_response_erase(broker_deque_of_store_response* d,
                                          size_t idx)
	{
	auto dd = reinterpret_cast<std::deque<broker::store::response>*>(d);
	dd->erase(dd->begin() + idx);
	}

int broker_store_response_queue_fd(const broker_store_response_queue* q)
	{
	auto qq = reinterpret_cast<const broker::store::response_queue*>(q);
	return qq->fd();
	}

broker_deque_of_store_response*
broker_store_response_queue_want_pop(const broker_store_response_queue* q)
	{
	auto rval = new (nothrow) std::deque<broker::store::response>;

	if ( ! rval )
		return nullptr;

	auto qq = reinterpret_cast<const broker::store::response_queue*>(q);
	*rval = qq->want_pop();
	return reinterpret_cast<broker_deque_of_store_response*>(rval);
	}

broker_deque_of_store_response*
broker_store_response_queue_need_pop(const broker_store_response_queue* q)
	{
	auto rval = new (nothrow) std::deque<broker::store::response>;

	if ( ! rval )
		return nullptr;

	auto qq = reinterpret_cast<const broker::store::response_queue*>(q);
	*rval = qq->need_pop();
	return reinterpret_cast<broker_deque_of_store_response*>(rval);
	}

broker_store_frontend*
broker_store_frontend_create(const broker_endpoint* e,
                             const broker_string* master_name)
	{
	auto ee = reinterpret_cast<const broker::endpoint*>(e);
	auto nn = reinterpret_cast<const std::string*>(master_name);

	try
		{
		auto rval = new broker::store::frontend(*ee, *nn);
		return reinterpret_cast<broker_store_frontend*>(rval);
		}
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

void broker_store_frontend_delete(broker_store_frontend* f)
	{
	delete reinterpret_cast<broker::store::frontend*>(f);
	}

const broker_string* broker_store_frontend_id(const broker_store_frontend* f)
	{
	auto ff = reinterpret_cast<const broker::store::frontend*>(f);
	return reinterpret_cast<const broker_string*>(&ff->id());
	}

const broker_store_response_queue*
broker_store_frontend_responses(const broker_store_frontend* f)
	{
	auto ff = reinterpret_cast<const broker::store::frontend*>(f);
	return reinterpret_cast<const broker_store_response_queue*>(
	            &ff->responses());
	}

int broker_store_frontend_insert(const broker_store_frontend* f,
                                 const broker_data* k,
                                 const broker_data* v)
	{
	auto ff = reinterpret_cast<const broker::store::frontend*>(f);
	auto kk = reinterpret_cast<const broker::data*>(k);
	auto vv = reinterpret_cast<const broker::data*>(v);

	try
		{
		ff->insert(*kk, *vv);
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

int
broker_store_frontend_insert_with_expiry(const broker_store_frontend* f,
                                         const broker_data* k,
                                         const broker_data* v,
                                         const broker_store_expiration_time* e)
	{
	auto ff = reinterpret_cast<const broker::store::frontend*>(f);
	auto kk = reinterpret_cast<const broker::data*>(k);
	auto vv = reinterpret_cast<const broker::data*>(v);
	auto ee = reinterpret_cast<const broker::store::expiration_time*>(e);

	try
		{
		ff->insert(*kk, *vv, *ee);
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

int broker_store_frontend_erase(const broker_store_frontend* f,
                                const broker_data* k)
	{
	auto ff = reinterpret_cast<const broker::store::frontend*>(f);
	auto kk = reinterpret_cast<const broker::data*>(k);

	try
		{
		ff->erase(*kk);
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

void broker_store_frontend_clear(const broker_store_frontend* f)
	{
	auto ff = reinterpret_cast<const broker::store::frontend*>(f);
	ff->clear();
	}

int broker_store_frontend_increment(const broker_store_frontend* f,
                                    const broker_data* k, int64_t by)
	{
	auto ff = reinterpret_cast<const broker::store::frontend*>(f);
	auto kk = reinterpret_cast<const broker::data*>(k);

	try
		{
		ff->increment(*kk, by);
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

int broker_store_frontend_add_to_set(const broker_store_frontend* f,
                                     const broker_data* k,
                                     const broker_data* element)
	{
	auto ff = reinterpret_cast<const broker::store::frontend*>(f);
	auto kk = reinterpret_cast<const broker::data*>(k);
	auto ee = reinterpret_cast<const broker::data*>(element);

	try
		{
		ff->add_to_set(*kk, *ee);
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

int broker_store_frontend_remove_from_set(const broker_store_frontend* f,
                                          const broker_data* k,
                                          const broker_data* element)
	{
	auto ff = reinterpret_cast<const broker::store::frontend*>(f);
	auto kk = reinterpret_cast<const broker::data*>(k);
	auto ee = reinterpret_cast<const broker::data*>(element);

	try
		{
		ff->remove_from_set(*kk, *ee);
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

int broker_store_frontend_push_left(const broker_store_frontend* f,
                                    const broker_data* k,
                                    const broker_vector* items)
	{
	auto ff = reinterpret_cast<const broker::store::frontend*>(f);
	auto kk = reinterpret_cast<const broker::data*>(k);
	auto ii = reinterpret_cast<const broker::vector*>(items);

	try
		{
		ff->push_left(*kk, *ii);
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

int broker_store_frontend_push_right(const broker_store_frontend* f,
                                     const broker_data* k,
                                     const broker_vector* items)
	{
	auto ff = reinterpret_cast<const broker::store::frontend*>(f);
	auto kk = reinterpret_cast<const broker::data*>(k);
	auto ii = reinterpret_cast<const broker::vector*>(items);

	try
		{
		ff->push_right(*kk, *ii);
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

broker_store_result*
broker_store_frontend_request_blocking(const broker_store_frontend* f,
                                       const broker_store_query* q)
	{
	auto ff = reinterpret_cast<const broker::store::frontend*>(f);
	auto qq = reinterpret_cast<const broker::store::query*>(q);

	try
		{
		auto rval = new broker::store::result;
		*rval = ff->request(*qq);
		return reinterpret_cast<broker_store_result*>(rval);
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

int broker_store_frontend_request_nonblocking(const broker_store_frontend* f,
                                              const broker_store_query* q,
                                              double timeout, void* cookie)
	{
	auto ff = reinterpret_cast<const broker::store::frontend*>(f);
	auto qq = reinterpret_cast<const broker::store::query*>(q);

	try
		{
		ff->request(*qq, std::chrono::duration<double>(timeout), cookie);
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}
