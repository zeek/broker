#ifndef BROKER_STORE_SQLITE_BACKEND_HH
#define BROKER_STORE_SQLITE_BACKEND_HH

#include <broker/store/backend.hh>

namespace broker { namespace store {

/**
 * A sqlite implementation of a storage backend.
 */
class sqlite_backend : public backend {
public:

	/**
	 * Constructor.  To open/create a database use sqlite_backend::open.
	 */
	sqlite_backend();

	/**
	 * Destructor.  Closes the database if open.
	 */
	~sqlite_backend();

	/**
	 * Construct sqlite backend by stealing another.
	 */
	sqlite_backend(sqlite_backend&&);

	/**
	 * Copying a sqlite backend is not allowed.
	 */
	sqlite_backend(sqlite_backend&) = delete;

	/**
	 * Replace sqlite backend by stealing another.
	 */
	sqlite_backend& operator=(sqlite_backend&&);

	/**
	 * Copying a sqlite backend is not allowed.
	 */
	sqlite_backend& operator=(sqlite_backend&) = delete;

	/**
	 * Open a sqlite database.
	 * @param db_path the filesystem path of the database to create/open.
	 * @param pragmas pragma statements to execute upon opening the database.
	 * @return true if the call succeeded.  If false last_error_code() and
	 * backend::last_error() may be used to obtain more info.
	 */
	bool open(std::string db_path,
	          std::deque<std::string> pragmas =
	                  {"pragma locking_mode = exclusive;"});

	/**
	 * Execute a pragma statement against the open database.
	 * @param p the pragma statement to execute.
	 * @return true if successful.  If false, last_error_code() and
	 * backend::last_error() may be used to obtain more info.
	 */
	bool pragma(std::string p);

	/**
	 * @return the last error code from a failed sqlite API call or a negative
	 * value if it was a non-sqlite failure.  In either case,
	 * backend::last_error() may be called to get a description of the error.
	 */
	int last_error_code() const;

private:

	void do_increase_sequence() override;

	std::string do_last_error() const override;

	bool do_init(snapshot sss) override;

	const sequence_num& do_sequence() const override;

	bool do_insert(data k, data v, util::optional<expiration_time> t) override;

	modification_result
	do_increment(const data& k, int64_t by, double mod_time) override;

	modification_result
	do_add_to_set(const data& k, data element, double mod_time) override;

	modification_result
	do_remove_from_set(const data& k, const data& element,
	                   double mod_time) override;

	bool do_erase(const data& k) override;

	bool do_expire(const data& k, const expiration_time& expiration) override;

	bool do_clear() override;

	modification_result
	do_push_left(const data& k, vector items, double mod_time) override;

	modification_result
	do_push_right(const data& k, vector items, double mod_time) override;

	std::pair<modification_result, util::optional<data>>
	do_pop_left(const data& k, double mod_time) override;

	std::pair<modification_result, util::optional<data>>
	do_pop_right(const data& k, double mod_time) override;

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

#endif // BROKER_STORE_SQLITE_BACKEND_HH
