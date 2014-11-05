#ifndef BROKER_STORE_BACKEND_HH
#define BROKER_STORE_BACKEND_HH

#include <broker/data.hh>
#include <broker/store/sequence_num.hh>
#include <broker/store/expiration_time.hh>
#include <broker/store/snapshot.hh>
#include <broker/util/optional.hh>
#include <unordered_map>
#include <unordered_set>
#include <deque>

namespace broker { namespace store {

/**
 * Abstract base class for a key-value storage backend.
 */
class backend {
public:

	/**
	 * Construct the storage backend, optionally specifying the starting
	 * sequence number.
	 */
	backend(sequence_num arg_sn = {})
		: sn(std::move(arg_sn))
		{}

	/**
	 * (Re-)Initialize storage backend from a snapshot of the desired contents.
	 */
	void init(snapshot sss)
		{
		sn = std::move(sss.sn);
		do_init(std::move(sss.datastore));
		}

	/**
	 * Destructor.
	 */
	virtual ~backend() { }

	/**
	 * @return a number indicating the current version of the store.
	 * Calls to the non-const methods of this class will increment this number.
	 */
	const sequence_num& sequence() const
		{ return sn; }

	/**
	 * Insert a key-value pair in to the store.
	 * @param k the key to use.
	 * @param v the value associated with the key.
	 * @param t an expiration time for the entry.
	 */
	void insert(data k, data v, util::optional<expiration_time> t = {})
		{ ++sn; do_insert(std::move(k), std::move(v), std::move(t)); }

	// TODO: increment/decrement

	/**
	 * Remove a key and its associated value from the store, if it exists.
	 * @param k the key to use.
	 */
	void erase(const data& k)
		{ ++sn; do_erase(k); }

	/**
	 * Remove all key-value pairs from the store.
	 */
	void clear()
		{ ++sn; do_clear(); }

	/**
	 * Lookup the value associated with a given key.
	 * @param k the key to use
	 * @return the value if the provided key exists.
	 */
	util::optional<data> lookup(const data& k) const
		{ return do_lookup(k); }

	/**
	 * Check if a given key exists.
	 * @param k the key to use.
	 * @return true if the provided key exists.
	 */
	bool exists(const data& k) const
		{ return do_exists(k); }

	/**
	 * @return all keys in the store.
	 */
	std::unordered_set<data> keys() const
		{ return do_keys(); }

	/**
	 * @return the number of key-value pairs in the store.
	 */
	uint64_t size() const
		{ return do_size(); }

	/**
	 * @return a snapshot of the store that includes its content as well as
	 * the sequence number associated with this snapshot of the content.
	 */
	snapshot snap() const
		{ return do_snap(); }

	std::deque<expirable> expiries() const
		{ return do_expiries(); }

private:

	virtual void do_init(std::unordered_map<data, value> datastore) = 0;

	virtual void do_insert(data k, data v,
	                       util::optional<expiration_time> t) = 0;

	virtual void do_erase(const data& k) = 0;

	virtual void do_clear() = 0;

	virtual util::optional<data> do_lookup(const data& k) const = 0;

	virtual bool do_exists(const data& k) const = 0;

	virtual std::unordered_set<data> do_keys() const = 0;

	virtual uint64_t do_size() const = 0;

	virtual snapshot do_snap() const = 0;

	virtual std::deque<expirable> do_expiries() const = 0;

	sequence_num sn;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_BACKEND_HH
