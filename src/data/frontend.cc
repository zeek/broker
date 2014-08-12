#include "frontend_impl.hh"
#include "query_types.hh"

#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <caf/sb_actor.hpp>
#include <caf/spawn.hpp>

static inline caf::actor& handle_to_actor(void* h)
	{
	return *static_cast<caf::actor*>(h);
	}

broker::data::frontend::frontend(const endpoint& e, std::string topic)
    : pimpl(std::make_shared<impl>(topic, handle_to_actor(e.handle()),
	                 subscription{subscription_type::data_query, topic},
	                 subscription{subscription_type::data_update, topic}))
	{
	}

broker::data::frontend::~frontend() = default;

const std::string& broker::data::frontend::topic() const
	{
	return pimpl->topic;
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

std::unique_ptr<broker::data::value> broker::data::frontend::lookup(key k) const
	{
	std::unique_ptr<value> rval;
	caf::scoped_actor self;
	self->sync_send(handle_to_actor(handle()),
	                lookup_request{pimpl->request_topic, std::move(k)}).await(
		caf::on_arg_match >> [&rval](value v)
			{
			rval.reset(new value(move(v)));
			},
		caf::on(caf::atom("null")) >> []
			{
			// Ok.  Return null pointer.
			}
	);
	return rval;
	}

bool broker::data::frontend::has_key(key k) const
	{
	bool rval = false;
	caf::scoped_actor self;
	self->sync_send(handle_to_actor(handle()),
	                has_key_request{pimpl->request_topic, std::move(k)}).await(
		caf::on_arg_match >> [&rval](bool hasit)
			{
			rval = hasit;
			}
	);
	return rval;
	}

std::unordered_set<broker::data::key> broker::data::frontend::keys() const
	{
	std::unordered_set<key> rval;
	caf::scoped_actor self;
	self->sync_send(handle_to_actor(handle()),
	                keys_request{pimpl->request_topic}).await(
		caf::on_arg_match >> [&rval](std::unordered_set<key> ks)
			{
			rval = std::move(ks);
			}
	);
	return rval;
	}

uint64_t broker::data::frontend::size() const
	{
	uint64_t rval = 0;
	caf::scoped_actor self;
	self->sync_send(handle_to_actor(handle()),
	                size_request{pimpl->request_topic}).await(
		caf::on_arg_match >> [&rval](uint64_t sz)
			{
			rval = sz;
			}
	);
	return rval;
	}

class async_lookup : public caf::sb_actor<async_lookup> {
friend class caf::sb_actor<async_lookup>;

public:

	async_lookup(caf::actor backend, broker::data::lookup_request req,
	             std::chrono::duration<double> timeout,
	             broker::data::LookupCallback cb, void* cookie)
		: request(std::move(req))
		{
		query = (
		caf::after(std::chrono::seconds::zero()) >> [=]
			{
			using val_ptr = std::unique_ptr<broker::data::value>;
			sync_send(backend, request).then(
				caf::on_arg_match >> [=](broker::data::value v)
					{
					cb(std::move(request.key),
					   val_ptr(new broker::data::value(std::move(v))), cookie,
					   broker::data::query_result::success);
					quit();
					},
				caf::on(caf::atom("null")) >> [=]
					{
					cb(std::move(request.key), val_ptr(), cookie,
					   broker::data::query_result::success);
					quit();
					},
				caf::on(caf::atom("dne")) >> [=]
					{
					cb(std::move(request.key), val_ptr(), cookie,
					   broker::data::query_result::nonexistent);
					quit();
					},
				caf::others() >> [=]
					{
					cb(std::move(request.key), val_ptr(), cookie,
					   broker::data::query_result::unknown_failure);
					quit();
					},
				caf::after(timeout) >> [=]
					{
					cb(std::move(request.key), val_ptr(), cookie,
					   broker::data::query_result::timeout);
					quit();
					}
			);
			}
		);
		}

private:

	broker::data::lookup_request request;
	caf::behavior query;
	caf::behavior& init_state = query;
};

void broker::data::frontend::lookup(key k,
                                    std::chrono::duration<double> timeout,
                                    LookupCallback cb, void* cookie) const
	{
	caf::spawn<async_lookup>(handle_to_actor(handle()),
	                         lookup_request{pimpl->request_topic, std::move(k)},
	                         timeout, cb, cookie);
	}

class async_haskey : public caf::sb_actor<async_haskey> {
friend class caf::sb_actor<async_haskey>;

public:

	async_haskey(caf::actor backend, broker::data::has_key_request req,
	             std::chrono::duration<double> timeout,
	             broker::data::HasKeyCallback cb, void* cookie)
		: request(std::move(req))
		{
		query = (
		caf::after(std::chrono::seconds::zero()) >> [=]
			{
			sync_send(backend, request).then(
				caf::on_arg_match >> [=](bool exists)
					{
					cb(std::move(request.key), exists, cookie,
					   broker::data::query_result::success);
					quit();
					},
				caf::on(caf::atom("dne")) >> [=]
					{
					cb(std::move(request.key), false, cookie,
					   broker::data::query_result::nonexistent);
					quit();
					},
				caf::others() >> [=]
					{
					cb(std::move(request.key), false, cookie,
					   broker::data::query_result::unknown_failure);
					quit();
					},
				caf::after(timeout) >> [=]
					{
					cb(std::move(request.key), false, cookie,
					   broker::data::query_result::timeout);
					quit();
					}
			);
			}
		);
		}

private:

	broker::data::has_key_request request;
	caf::behavior query;
	caf::behavior& init_state = query;
};

void broker::data::frontend::has_key(key k,
                                     std::chrono::duration<double> timeout,
                                     HasKeyCallback cb, void* cookie) const
	{
	caf::spawn<async_haskey>(handle_to_actor(handle()),
	                         has_key_request{pimpl->request_topic, std::move(k)},
	                         timeout, cb, cookie);
	}

class async_keys : public caf::sb_actor<async_keys> {
friend class caf::sb_actor<async_keys>;

public:

	async_keys(caf::actor backend, broker::data::keys_request req,
	           std::chrono::duration<double> timeout,
	           broker::data::KeysCallback cb, void* cookie)
		: request(std::move(req))
		{
		query = (
		caf::after(std::chrono::seconds::zero()) >> [=]
			{
			using keyset = std::unordered_set<broker::data::key>;
			sync_send(backend, request).then(
				caf::on_arg_match >> [=](keyset keys)
					{
					cb(std::move(keys), cookie,
					   broker::data::query_result::success);
					quit();
					},
				caf::on(caf::atom("dne")) >> [=]
					{
					cb({}, cookie, broker::data::query_result::nonexistent);
					quit();
					},
				caf::others() >> [=]
					{
					cb({}, cookie,
					   broker::data::query_result::unknown_failure);
					quit();
					},
				caf::after(timeout) >> [=]
					{
					cb({}, cookie, broker::data::query_result::timeout);
					quit();
					}
			);
			}
		);
		}

private:

	broker::data::keys_request request;
	caf::behavior query;
	caf::behavior& init_state = query;
};

void broker::data::frontend::keys(std::chrono::duration<double> timeout,
                                  KeysCallback cb, void* cookie) const
	{
	caf::spawn<async_keys>(handle_to_actor(handle()),
	                       keys_request{pimpl->request_topic},
	                       timeout, cb, cookie);
	}

class async_size : public caf::sb_actor<async_size> {
friend class caf::sb_actor<async_size>;

public:

	async_size(caf::actor backend, broker::data::size_request req,
	           std::chrono::duration<double> timeout,
	           broker::data::SizeCallback cb, void* cookie)
		: request(std::move(req))
		{
		query = (
		caf::after(std::chrono::seconds::zero()) >> [=]
			{
			sync_send(backend, request).then(
				caf::on_arg_match >> [=](uint64_t sz)
					{
					cb(sz, cookie, broker::data::query_result::success);
					quit();
					},
				caf::on(caf::atom("dne")) >> [=]
					{
					cb(0, cookie, broker::data::query_result::nonexistent);
					quit();
					},
				caf::others() >> [=]
					{
					cb(0, cookie,
					   broker::data::query_result::unknown_failure);
					quit();
					},
				caf::after(timeout) >> [=]
					{
					cb(0, cookie, broker::data::query_result::timeout);
					quit();
					}
			);
			}
		);
		}

private:

	broker::data::size_request request;
	caf::behavior query;
	caf::behavior& init_state = query;
};

void broker::data::frontend::size(std::chrono::duration<double> timeout,
                                  SizeCallback cb, void* cookie) const
	{
	caf::spawn<async_size>(handle_to_actor(handle()),
	                       size_request{pimpl->request_topic},
	                       timeout, cb, cookie);
	}

void* broker::data::frontend::handle() const
	{
	return &pimpl->endpoint;
	}
