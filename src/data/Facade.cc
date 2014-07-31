#include "FacadeImpl.hh"
#include "../EndpointImpl.hh"
#include "RequestMsgs.hh"

#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <caf/sb_actor.hpp>
#include <caf/spawn.hpp>

static inline caf::actor* handle_to_actor(void* backend)
	{
	return static_cast<caf::actor*>(backend);
	}

broker::data::Facade::Facade(const Endpoint &e, std::string topic)
    : p(new Impl{topic, e.p->endpoint,
	             {SubscriptionType::DATA_REQUEST, topic},
	             {SubscriptionType::DATA_UPDATE, topic}})
	{
	}

broker::data::Facade::~Facade() = default;

const std::string& broker::data::Facade::Topic() const
	{
	return p->topic;
	}

void broker::data::Facade::Insert(Key k, Val v) const
	{
	caf::anon_send(*handle_to_actor(GetBackendHandle()),
	               p->data_topic, caf::atom("insert"),
	               std::move(k), std::move(v));
	}

void broker::data::Facade::Erase(Key k) const
	{
	caf::anon_send(*handle_to_actor(GetBackendHandle()),
	               p->data_topic, caf::atom("erase"),
	               std::move(k));
	}

void broker::data::Facade::Clear() const
	{
	caf::anon_send(*handle_to_actor(GetBackendHandle()),
	               p->data_topic, caf::atom("clear"));
	}

std::unique_ptr<broker::data::Val> broker::data::Facade::Lookup(Key k) const
	{
	std::unique_ptr<Val> rval;
	caf::scoped_actor self;
	self->sync_send(*handle_to_actor(GetBackendHandle()),
	                LookupRequest{p->request_topic, std::move(k)}).await(
		caf::on_arg_match >> [&rval](Val v)
			{
			rval.reset(new Val(move(v)));
			}
	);
	return rval;
	}

bool broker::data::Facade::HasKey(Key k) const
	{
	bool rval = false;
	caf::scoped_actor self;
	self->sync_send(*handle_to_actor(GetBackendHandle()),
	                HasKeyRequest{p->request_topic, std::move(k)}).await(
		caf::on_arg_match >> [&rval](bool hasit)
			{
			rval = hasit;
			}
	);
	return rval;
	}

std::unordered_set<broker::data::Key> broker::data::Facade::Keys() const
	{
	std::unordered_set<Key> rval;
	caf::scoped_actor self;
	self->sync_send(*handle_to_actor(GetBackendHandle()),
	                KeysRequest{p->request_topic}).await(
		caf::on_arg_match >> [&rval](std::unordered_set<Key> ks)
			{
			rval = std::move(ks);
			}
	);
	return rval;
	}

uint64_t broker::data::Facade::Size() const
	{
	uint64_t rval = 0;
	caf::scoped_actor self;
	self->sync_send(*handle_to_actor(GetBackendHandle()),
	                SizeRequest{p->request_topic}).await(
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

	async_lookup(caf::actor backend, broker::data::LookupRequest req,
	             broker::data::LookupCallback cb, void* cookie)
		: request(std::move(req))
		{
		query = (
		caf::after(std::chrono::seconds::zero()) >> [=]
			{
			std::unique_ptr<broker::data::Val> rval;
			sync_send(backend, request).then(
				caf::on_arg_match >> [&rval](broker::data::Val v)
					{
					rval.reset(new broker::data::Val(std::move(v)));
					}
			);
			cb(std::move(request.key), std::move(rval), cookie);
			quit();
			}
		);
		}

private:

	broker::data::LookupRequest request;
	caf::behavior query;
	caf::behavior& init_state = query;
};

void broker::data::Facade::Lookup(Key k, LookupCallback cb, void* cookie) const
	{
	caf::spawn<async_lookup>(*handle_to_actor(GetBackendHandle()),
	                         LookupRequest{p->request_topic, std::move(k)},
	                         cb, cookie);
	}

class async_haskey : public caf::sb_actor<async_haskey> {
friend class caf::sb_actor<async_haskey>;

public:

	async_haskey(caf::actor backend, broker::data::HasKeyRequest req,
	             broker::data::HasKeyCallback cb, void* cookie)
		: request(std::move(req))
		{
		query = (
		caf::after(std::chrono::seconds::zero()) >> [=]
			{
			bool rval = false;
			sync_send(backend, request).then(
				caf::on_arg_match >> [&rval](bool hasit)
					{
					rval = hasit;
					}
			);
			cb(std::move(request.key), rval, cookie);
			quit();
			}
		);
		}

private:

	broker::data::HasKeyRequest request;
	caf::behavior query;
	caf::behavior& init_state = query;
};

void broker::data::Facade::HasKey(Key k, HasKeyCallback cb, void* cookie) const
	{
	caf::spawn<async_haskey>(*handle_to_actor(GetBackendHandle()),
	                         HasKeyRequest{p->request_topic, std::move(k)},
	                         cb, cookie);
	}

class async_keys : public caf::sb_actor<async_keys> {
friend class caf::sb_actor<async_keys>;

public:

	async_keys(caf::actor backend, broker::data::KeysRequest req,
	           broker::data::KeysCallback cb, void* cookie)
		: request(std::move(req))
		{
		query = (
		caf::after(std::chrono::seconds::zero()) >> [=]
			{
			using keyset = std::unordered_set<broker::data::Key>;
			keyset rval;
			sync_send(backend, request).then(
				caf::on_arg_match >> [&rval](keyset keys)
					{
					rval = std::move(keys);
					}
			);
			cb(std::move(rval), cookie);
			quit();
			}
		);
		}

private:

	broker::data::KeysRequest request;
	caf::behavior query;
	caf::behavior& init_state = query;
};

void broker::data::Facade::Keys(KeysCallback cb, void* cookie) const
	{
	caf::spawn<async_keys>(*handle_to_actor(GetBackendHandle()),
	                        KeysRequest{p->request_topic}, cb, cookie);
	}

class async_size : public caf::sb_actor<async_size> {
friend class caf::sb_actor<async_size>;

public:

	async_size(caf::actor backend, broker::data::SizeRequest req,
	           broker::data::SizeCallback cb, void* cookie)
		: request(std::move(req))
		{
		query = (
		caf::after(std::chrono::seconds::zero()) >> [=]
			{
			uint64_t rval = 0;
			sync_send(backend, request).then(
				caf::on_arg_match >> [&rval](uint64_t sz)
					{
					rval = sz;
					}
			);
			cb(rval, cookie);
			quit();
			}
		);
		}

private:

	broker::data::SizeRequest request;
	caf::behavior query;
	caf::behavior& init_state = query;
};

void broker::data::Facade::Size(SizeCallback cb, void* cookie) const
	{
	caf::spawn<async_size>(*handle_to_actor(GetBackendHandle()),
	                       SizeRequest{p->request_topic}, cb, cookie);
	}

void* broker::data::Facade::GetBackendHandle() const
	{
	return &p->endpoint;
	}
