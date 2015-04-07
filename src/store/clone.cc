#include "clone_impl.hh"

broker::store::clone::clone(const endpoint& e, identifier master_name,
                            std::chrono::duration<double> ri,
                            std::unique_ptr<backend> b)
	: broker::store::frontend(e, master_name),
      pimpl(new impl(*static_cast<caf::actor*>(e.handle()),
                     std::move(master_name),
                     std::chrono::duration_cast<std::chrono::microseconds>(ri),
                     std::move(b)))
	{
	}

broker::store::clone::~clone() = default;

broker::store::clone::clone(clone&& other) = default;

broker::store::clone& broker::store::clone::operator=(clone&& other) = default;

void* broker::store::clone::handle() const
	{
	return &pimpl->actor;
	}

// Begin C API
#include "broker/broker.h"

broker_store_frontend*
broker_store_clone_create_memory(const broker_endpoint* e,
                                 const broker_string* name,
                                 double resync_interval)
	{
	auto ee = reinterpret_cast<const broker::endpoint*>(e);
	auto nn = reinterpret_cast<const std::string*>(name);
	auto rr = std::chrono::duration<double>(resync_interval);

	try
		{
		auto rval = new broker::store::clone(*ee, *nn, rr);
		return reinterpret_cast<broker_store_frontend*>(rval);
		}
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

broker_store_frontend*
broker_store_clone_create_sqlite(const broker_endpoint* e,
                                 const broker_string* name,
                                 double resync_interval,
                                 broker_store_sqlite_backend* b)
	{
	auto ee = reinterpret_cast<const broker::endpoint*>(e);
	auto nn = reinterpret_cast<const std::string*>(name);
	auto bb = reinterpret_cast<broker::store::sqlite_backend*>(b);
	auto rr = std::chrono::duration<double>(resync_interval);

	try
		{
		std::unique_ptr<broker::store::backend> bp(bb);
		auto rval = new broker::store::clone(*ee, *nn, rr, std::move(bp));
		return reinterpret_cast<broker_store_frontend*>(rval);
		}
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

#ifdef HAVE_ROCKSDB
#include "broker/store/rocksdb_backend.hh"

broker_store_frontend*
broker_store_clone_create_rocksdb(const broker_endpoint* e,
                                  const broker_string* name,
                                  double resync_interval,
                                  broker_store_rocksdb_backend* b)
	{
	auto ee = reinterpret_cast<const broker::endpoint*>(e);
	auto nn = reinterpret_cast<const std::string*>(name);
	auto bb = reinterpret_cast<broker::store::rocksdb_backend*>(b);
	auto rr = std::chrono::duration<double>(resync_interval);

	try
		{
		std::unique_ptr<broker::store::backend> bp(bb);
		auto rval = new broker::store::clone(*ee, *nn, rr, std::move(bp));
		return reinterpret_cast<broker_store_frontend*>(rval);
		}
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

#endif // HAVE_ROCKSDB
