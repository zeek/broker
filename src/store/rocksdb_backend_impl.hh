#ifndef BROKER_STORE_ROCKSDB_BACKEND_IMPL_HH
#define BROKER_STORE_ROCKSDB_BACKEND_IMPL_HH

#include "broker/store/rocksdb_backend.hh"
#include <rocksdb/db.h>

namespace broker {
namespace store {

class rocksdb_backend::impl {
public:

	impl(uint64_t size_threshold)
		: exact_size_threshold(size_threshold)
		{}

	bool require_db()
		{
		if ( db )
			return true;

		last_error = "db not open";
		return false;
		}

	bool require_ok(const rocksdb::Status& s)
		{
		if ( s.ok() )
			return true;

		last_error = s.ToString();
		return false;
		}

	sequence_num sn;
	std::string last_error;
	std::unique_ptr<rocksdb::DB> db;
	rocksdb::Options options;
	uint64_t exact_size_threshold;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_ROCKSDB_BACKEND_IMPL_HH
