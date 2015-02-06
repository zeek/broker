#ifndef BROKER_STORE_ROCKSDB_BACKEND_HH
#define BROKER_STORE_ROCKSDB_BACKEND_HH

#include <broker/store/backend.hh>
#include <rocksdb/options.h>
#include <rocksdb/status.h>

namespace broker { namespace store {

/**
 * A RocksDB implementation of a storage backend.  The results of
 * backend::size() should be interpreted as estimations.
 */
class rocksdb_backend : public backend {
public:

	/**
	 * Constructor.  To open/create a database use rocksdb_backend::open.
	 * @param exact_size_threshold when the number of keys is estimated to
	 * be below this number, the exact number is counted via iteration, else
	 * the estimate is used as a return value to backend::size().  The estimate
	 * may double-count keys that have expiration times.
	 */
	rocksdb_backend(uint64_t exact_size_threshold = 1000);

	/**
	 * Destructor.  Closes the database if open.
	 */
	~rocksdb_backend();

	/**
	 * Construct rocksdb backend by stealing another.
	 */
	rocksdb_backend(rocksdb_backend&&);

	/**
	 * Copying a rocksdb backend is not allowed.
	 */
	rocksdb_backend(rocksdb_backend&) = delete;

	/**
	 * Replace rocksdb backend by stealing another.
	 */
	rocksdb_backend& operator=(rocksdb_backend&&);

	/**
	 * Copying a rocksdb backend is not allowed.
	 */
	rocksdb_backend& operator=(rocksdb_backend&) = delete;

	/**
	 * Open a rocksdb database.
	 * @param db_path the file system directory to use for the database.
	 * @param options object containing parameters to use for the database.
	 * @return the success status of opening the database.
	 */
	rocksdb::Status open(std::string db_path, rocksdb::Options options = {});

private:

	void do_increase_sequence() override;

	std::string do_last_error() const override;

	bool do_init(snapshot sss) override;

	const sequence_num& do_sequence() const override;

	bool do_insert(data k, data v, util::optional<expiration_time> t) override;

	modification_result
	do_increment(const data& k, int64_t by, double time) override;

	modification_result
	do_add_to_set(const data& k, data element, double time) override;

	modification_result
	do_remove_from_set(const data& k, const data& element,
	                   double time) override;

	bool do_erase(const data& k) override;

	bool do_erase(std::string kserial);

	bool do_expire(const data& k, const expiration_time& expiration) override;

	bool do_clear() override;

	modification_result
	do_push_left(const data& k, vector items, double time) override;

	modification_result
	do_push_right(const data& k, vector items, double time) override;

	std::pair<modification_result, util::optional<data>>
	do_pop_left(const data& k, double time) override;

	std::pair<modification_result, util::optional<data>>
	do_pop_right(const data& k, double time) override;

	util::optional<util::optional<data>>
	do_lookup(const data& k) const override;

	util::optional<std::pair<util::optional<data>,
	               util::optional<expiration_time>>>
	do_lookup_expiry(const data& k) const;

	util::optional<bool> do_exists(const data& k) const override;

	util::optional<std::vector<data>> do_keys() const override;

	util::optional<uint64_t> do_size() const override;

	util::optional<snapshot> do_snap() const override;

	util::optional<std::deque<expirable>> do_expiries() const override;

	class impl;
	std::unique_ptr<impl> pimpl;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_ROCKSDB_BACKEND_HH
