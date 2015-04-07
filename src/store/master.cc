#include "master_impl.hh"

broker::store::master::master(const endpoint& e, identifier name,
                              std::unique_ptr<backend> s)
    : broker::store::frontend(e, name),
      pimpl(new impl(*static_cast<caf::actor*>(e.handle()),
                     std::move(name), std::move(s)))
	{
	}

broker::store::master::~master() = default;

broker::store::master::master(master&& other) = default;

broker::store::master&
broker::store::master::operator=(master&& other) = default;

void* broker::store::master::handle() const
	{
	return &pimpl->actor;
	}

// Begin C API
#include "broker/broker.h"

broker_store_frontend*
broker_store_master_create_memory(const broker_endpoint* e,
                                  const broker_string* name)
	{
	auto ee = reinterpret_cast<const broker::endpoint*>(e);
	auto nn = reinterpret_cast<const std::string*>(name);

	try
		{
		auto rval = new broker::store::master(*ee, *nn);
		return reinterpret_cast<broker_store_frontend*>(rval);
		}
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

broker_store_frontend*
broker_store_master_create_sqlite(const broker_endpoint* e,
                                  const broker_string* name,
                                  broker_store_sqlite_backend* b)
	{
	auto ee = reinterpret_cast<const broker::endpoint*>(e);
	auto nn = reinterpret_cast<const std::string*>(name);
	auto bb = reinterpret_cast<broker::store::sqlite_backend*>(b);

	try
		{
		std::unique_ptr<broker::store::backend> bp(bb);
		auto rval = new broker::store::master(*ee, *nn, std::move(bp));
		return reinterpret_cast<broker_store_frontend*>(rval);
		}
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

#ifdef HAVE_ROCKSDB
#include "broker/store/rocksdb_backend.hh"

broker_store_frontend*
broker_store_master_create_rocksdb(const broker_endpoint* e,
                                   const broker_string* name,
                                   broker_store_rocksdb_backend* b)
	{
	auto ee = reinterpret_cast<const broker::endpoint*>(e);
	auto nn = reinterpret_cast<const std::string*>(name);
	auto bb = reinterpret_cast<broker::store::rocksdb_backend*>(b);

	try
		{
		std::unique_ptr<broker::store::backend> bp(bb);
		auto rval = new broker::store::master(*ee, *nn, std::move(bp));
		return reinterpret_cast<broker_store_frontend*>(rval);
		}
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

#endif // HAVE_ROCKSDB
