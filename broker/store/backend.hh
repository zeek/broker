#ifndef BROKER_STORE_BACKEND_HH
#define BROKER_STORE_BACKEND_HH

#include <broker/data.hh>
#include <broker/store/sequence_num.hh>
#include <broker/store/expiration_time.hh>
#include <broker/store/snapshot.hh>
#include <broker/util/optional.hh>
#include <deque>
#include <vector>

namespace broker { namespace store {

class modification_result {
public:

	enum class status : uint8_t {
		// Everything worked.
		success,
		// Fundamental issue w/ the backend prevented operation from completing.
		failure,
		// The operation was invalid (e.g. trying to apply a set operation to
		// an integer) and the backend is left unchanged.
		invalid,
	} stat;

	// New expiration parameters, if modifying the entry changed them.
	util::optional<expiration_time> new_expiration;
};

/**
 * Abstract base class for a key-value storage backend.
 */
class backend {
public:

	/**
	 * Destructor.
	 */
	virtual ~backend();

	/**
	 * @return a description of the last error/failure that occurred, which
	 * may be undefined if no method call ever failed or a method was successful
	 * between the time of the last failure and call to this method.
	 */
	std::string last_error() const;

	/**
	 * (Re-)Initialize storage backend from a snapshot of the desired contents.
	 * @return true on success.
	 */
	bool init(snapshot sss);

	/**
	 * @return a number indicating the current version of the store.
	 * Calls to the non-const methods of this class will increment this number.
	 */
	const sequence_num& sequence() const;

	/**
	 * Insert a key-value pair in to the store.
	 * @param k the key to use.
	 * @param v the value associated with the key.
	 * @param t an expiration time for the entry.
	 * @return true on success.
	 */
	bool insert(data k, data v, util::optional<expiration_time> t = {});

	/**
	 * Increment an integral value by a certain amount.
	 * @param k the key associated with an integral value to increment.
	 * @param by the size of the increment to take.
	 * @param mod_time the epoch time this modification is taking place.
	 * @return the result of the modification.
	 */
	modification_result
	increment(const data& k, int64_t by, double mod_time);

	/**
	 * Add an element to a set.
	 * @param k the key associated with the set to modify.
	 * @param element the element to add to the set.
	 * @param mod_time the epoch time this modification is taking place.
	 * @return the result of the modification.
	 */
	modification_result
	add_to_set(const data& k, data element, double mod_time);

	/**
	 * Remove an element from a set.
	 * @param k the key associated with the set to modify.
	 * @param element the element to remove from the set.
	 * @param mod_time the epoch time this modification is taking place.
	 * @return the result of the modification.
	 */
	modification_result
	remove_from_set(const data& k, const data& element, double mod_time);

	/**
	 * Remove a key and its associated value from the store, if it exists.
	 * @param k the key to use.
	 * @return true if the key didn't exist or was removed successfully.
	 */
	bool erase(const data& k);

	/**
	 * Remove a key and its associated value from the store, if it exists and
	 * the expiration value is the same.
	 * @param k the key to use.
	 * @param expiration the expiration value which must still match, otherwise
	 * this expiry operation is ignored.
	 * @return true if the key didn't exist or was removed successfully.
	 */
	bool expire(const data& k, const expiration_time& expiration);

	/**
	 * Remove all key-value pairs from the store.
	 * @return true on success.
	 */
	bool clear();

	/**
	 * Push items to the head of a vector.
	 * @param k the key associated with the vector to modify.
	 * @param items the items to add to the vector.
	 * @param mod_time the epoch time this modification is taking place.
	 * @return the result of the modification.
	 */
	modification_result
	push_left(const data& k, vector items, double mod_time);

	/**
	 * Push items to the tail of a vector.
	 * @param k the key associated with the vector to modify.
	 * @param items the items to add to the vector.
	 * @param mod_time the epoch time this modification is taking place.
	 * @return The result of the modification.
	 */
	modification_result
	push_right(const data& k, vector items, double mod_time);

	/**
	 * Retrieve item at the head of a vector value associated with a given key.
	 * @param k the key to use.
	 * @param mod_time the epoch time this modification is taking place.
	 * @return the result of the modification along with the item if the
	 * provided key exists or nil on failing to perform the query.
	 */
	std::pair<modification_result, util::optional<data>>
	pop_left(const data& k, double mod_time);

	/**
	 * Retrieve item at the tail of a vector value associated with a given key.
	 * @param k the key to use.
	 * @param mod_time the epoch time this modification is taking place.
	 * @return the result of the modification along with the item if the
	 * provided key exists or nil on failing to perform the query.
	 */
	std::pair<modification_result, util::optional<data>>
	pop_right(const data& k, double mod_time);

	/**
	 * Lookup the value associated with a given key.
	 * @param k the key to use.
	 * @return the value if the provided key exists or nil on failing to perform
	 * the query.
	 */
	util::optional<util::optional<data>> lookup(const data& k) const;

	/**
	 * Check if a given key exists.
	 * @param k the key to use.
	 * @return true if the provided key exists or nil on failing to perform
	 * the query.
	 */
	util::optional<bool> exists(const data& k) const;

	/**
	 * @return all keys in the store or nil on failing to perform the query.
	 */
	util::optional<std::vector<data>> keys() const;

	/**
	 * @return the number of key-value pairs in the store or nil on failing
	 * to perform the query.  Depending on the choice of storage backend,
	 * this may be an approximation.
	 */
	util::optional<uint64_t> size() const;

	/**
	 * @return a snapshot of the store that includes its content as well as
	 * the sequence number associated with this snapshot of the content or
	 * nil on failing to perform the query.
	 */
	util::optional<snapshot> snap() const;

	/**
	 * @return all the keys in the datastore that have an expiration time or
	 * nil on failing to perform the query.
	 */
	util::optional<std::deque<expirable>> expiries() const;

private:

	virtual void do_increase_sequence() = 0;

	virtual std::string do_last_error() const = 0;

	virtual bool do_init(snapshot sss) = 0;

	virtual const sequence_num& do_sequence() const = 0;

	virtual bool do_insert(data k, data v,
	                       util::optional<expiration_time> t) = 0;

	virtual modification_result
	do_increment(const data& k, int64_t by, double mod_time) = 0;

	virtual modification_result
	do_add_to_set(const data& k, data element, double mod_time) = 0;

	virtual modification_result
	do_remove_from_set(const data& k, const data& element, double mod_time) = 0;

	virtual bool do_erase(const data& k) = 0;

	virtual bool
	do_expire(const data& k, const expiration_time& expiration) = 0;

	virtual bool do_clear() = 0;

	virtual modification_result
	do_push_left(const data& k, vector items, double mod_time) = 0;

	virtual modification_result
	do_push_right(const data& k, vector items, double mod_time) = 0;

	virtual std::pair<modification_result, util::optional<data>>
	do_pop_left(const data& k, double mod_time) = 0;

	virtual std::pair<modification_result, util::optional<data>>
	do_pop_right(const data& k, double mod_time) = 0;

	virtual util::optional<util::optional<data>>
	do_lookup(const data& k) const = 0;

	virtual util::optional<bool> do_exists(const data& k) const = 0;

	virtual util::optional<std::vector<data>> do_keys() const = 0;

	virtual util::optional<uint64_t> do_size() const = 0;

	virtual util::optional<snapshot> do_snap() const = 0;

	virtual util::optional<std::deque<expirable>> do_expiries() const = 0;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_BACKEND_HH
