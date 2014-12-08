#ifndef BROKER_STORE_FRONTEND_HH
#define BROKER_STORE_FRONTEND_HH

#include <broker/data.hh>
#include <broker/queue.hh>
#include <broker/store/response.hh>
#include <broker/store/identifier.hh>
#include <broker/store/expiration_time.hh>
#include <broker/util/optional.hh>
#include <broker/endpoint.hh>
#include <string>
#include <chrono>

namespace broker { namespace store {

using response_queue = broker::queue<broker::store::response>;

/**
 * A frontend interface of a data store (either a master or clone)
 * that allows querying and updating contents.
 */
class frontend {
public:

	/**
	 * Construct a data store frontend to a master data store.
	 * @param e the broker endpoint to attach the frontend.
	 * @param name the exact name that a master data store is using.
	 * The master store must be attached either directly to the same endpoint as
	 * the to one of its peers.  If attached to a peer, the endpoint must
	 * allow advertising interest in this name.
	 */
	frontend(const endpoint& e, identifier master_name);

	/**
	 * Destructor.
	 */
	virtual ~frontend();

	/**
	 * Copying a frontend is not allowed.
	 */
	frontend(const frontend& other) = delete;

	/**
	 * Construct a frontend by stealing another.
	 */
	frontend(frontend&& other);

	/**
	 * Copying a frontend is not allowed.
	 */
	frontend& operator=(const frontend& other) = delete;

	/**
	 * Assign to a frontend by stealing another.
	 */
	frontend& operator=(frontend&& other);

	/**
	 * @return the name of the associated master data store.
	 */
	const identifier& id() const;

	/**
	 * @return a queue that contains responses to queries that await processing.
	 */
	const response_queue& responses() const;

	/*
	 * Update Interface - non-blocking.
	 * Changes may not be immediately visible.
	 */

	/**
	 * Non-blocking key-value pair insertion.
	 * The change may not be immediately visible.
	 * @param k the key to use.
	 * @param v the value associated with the key.
	 * @param t an expiration time for the entry.
	 */
	void insert(data k, data v, util::optional<expiration_time> t = {}) const;

	/**
	 * Non-blocking removal of a key and associated value, if it exists.
	 * The change may not be immediately visible.
	 * @param k
	 */
	void erase(data k) const;

	/**
	 * Non-blocking removal of all key-value pairs in the store.
	 * The change may not be immediately visible.
	 */
	void clear() const;

	/**
	 * Increment an integral value by a certain amount.
	 * @param k the key associated with an integral value to increment.  If
	 * the value associated with the key is not integral, no operation takes
	 * place and an error message is generated.  If the key does not exist, it
	 * is implicitly created with a value of zero.
	 * @param by the size of the increment to take.
	 */
	void increment(data k, int64_t by = 1) const;

	/**
	 * Decrement an integral value by a certain amount.
	 * @param k the key associated with an integral value to increment.  If
	 * the value associated with the key is not integral, no operation takes
	 * place and an error message is generated.  If the key does not exist, it
	 * is implicitly created with a value of zero.
	 * @param by the size of the decrement to take.
	 */
	void decrement(data k, int64_t by = 1) const;

	/**
	 * Add a new element to a set.
	 * @param k the key associated with the set to modify.  If the value
	 * associated with the key is not a set, no operation takes place and an
	 * error message is generated.  If the key does not exist, it is implicitly
	 * created as an empty set.
	 * @param element the new element to add to the set (if not already in it).
	 */
	void add_to_set(data k, data element) const;

	/**
	 * Remove an element from a set.
	 * @param k the key associated with the set to modify.  If the value
	 * associated with the key is not a set, no operation takes place and an
	 * error message is generated.  If the key does not exist, it is implicitly
	 * created as an empty set.
	 * @param element the element to remove from the set (if it is in it).
	 */
	void remove_from_set(data k, data element) const;

	/*
	 * Query Interface - blocking.
	 * May have high latency if data is non-local.
	 */

	/**
	 * Make a query and block until response is received.
	 * May have high latency if data is non-local.
	 * @return the result of the query.
	 */
	result request(query q) const;

	/**
	 * Make a query and block until response is received.
	 * May have high latency if data is non-local.
	 * @param k the key to lookup for its corresponding value.
	 * @return the result of the query.
	 */
	result lookup(data k) const
		{ return request(query(query::tag::lookup, std::move(k))); }

	/**
	 * Make a query and block until response is received.
	 * May have high latency if data is non-local.
	 * @param k the key to check for existence.
	 * @return the result of the query.
	 */
	result exists(data k) const
		{ return request(query(query::tag::exists, std::move(k))); }

	/**
	 * Make a query and block until response is received.
	 * May have high latency if data is non-local.
	 * @return the result of the query -- all keys in the data store.
	 */
	result keys() const
		{ return request(query(query::tag::keys)); }

	/**
	 * Make a query and block until response is received.
	 * May have high latency if data is non-local.
	 * @return the result of the query -- the number of key-value pairs in the
	 * data store.
	 */
	result size() const
		{ return request(query(query::tag::size)); }

	/*
	 * Query Interface - non-blocking.
	 */

	/**
	 * Make a non-blocking query.
	 * @param q the query.
	 * @param timeout the amount of time after which the query times out.
	 * @param cookie a pointer value to make available in the result/response
	 * when it is available.
	 */
	void request(query q, std::chrono::duration<double> timeout,
	             void* cookie = nullptr) const;

	/**
	 * Make a non-blocking query for the value associated with a key.
	 * @param k the key to lookup for its corresponding value.
	 * @param timeout the amount of time after which the query times out.
	 * @param cookie a pointer value to make available in the result/response
	 * when it is available.
	 */
	void lookup(data k, std::chrono::duration<double> timeout,
	            void* cookie = nullptr) const
		{ request(query(query::tag::lookup, std::move(k)), timeout, cookie); }

	/**
	 * Make a non-blocking query to check for a key's existence..
	 * @param k the key to check for existence.
	 * @param timeout the amount of time after which the query times out.
	 * @param cookie a pointer value to make available in the result/response
	 * when it is available.
	 */
	void exists(data k, std::chrono::duration<double> timeout,
	             void* cookie = nullptr) const
		{ request(query(query::tag::exists, std::move(k)), timeout, cookie); }

	/**
	 * Make a non-blocking query to obtain all keys in the store.
	 * @param timeout the amount of time after which the query times out.
	 * @param cookie a pointer value to make available in the result/response
	 * when it is available.
	 */
	void keys(std::chrono::duration<double> timeout,
	          void* cookie = nullptr) const
		{ request(query(query::tag::keys), timeout, cookie); }

	/**
	 * Make a non-blocking query to obtain the number of key-value pairs in the
	 * store.
	 * @param timeout the amount of time after which the query times out.
	 * @param cookie a pointer value to make available in the result/response
	 * when it is available.
	 */
	void size(std::chrono::duration<double> timeout,
	          void* cookie = nullptr) const
		{ request(query(query::tag::size), timeout, cookie); }

private:

	virtual void* handle() const;

	class impl;
	std::unique_ptr<impl> pimpl;
};

/**
 * Blocking lookup of a key in a data store.
 * @tparam T a class that supports the frontend interface.
 * @param f the frontend to use.
 * @param k the key to lookup.
 * @return the associated value if the key existed.
 */
template <typename T>
util::optional<data> lookup(const T& f, data k)
	{
	result r = f.lookup(std::move(k));

	if ( r.stat != result::status::success )
		return {};

	auto p = util::get<data>(r.value);

	if ( p )
		return std::move(*p);

	return {};
	}

/**
 * Blocking existence check of a key in a data store.
 * @tparam T a class that supports the frontend interface.
 * @param f the frontend to use.
 * @param k the key to check.
 * @return true if the key existed.
 */
template <typename T>
bool exists(const T& f, data k)
	{
	result r = f.exists(std::move(k));

	if ( r.stat != result::status::success )
		return false;

	return *util::get<bool>(r.value);
	}

/**
 * Blocking key retrieval for all keys in a data store.
 * @tparam T a class that supports the frontend interface.
 * @param f the frontend to use.
 * @return all keys in the data store.
 */
template <typename T>
std::vector<data> keys(const T& f)
	{
	result r = f.keys();

	if ( r.stat != result::status::success )
		return {};

	return *util::get<std::vector<data>>(r.value);
	}

/**
 * Blocking query for the number of key-value pairs in a data store.
 * @tparam T a class that supports the frontend interface.
 * @param f the frontend to use.
 * @return the number of key-value pairs in the data store.
 */
template <typename T>
uint64_t size(const T& f)
	{
	result r = f.size();

	if ( r.stat != result::status::success )
		return 0;

	return *util::get<uint64_t>(r.value);
	}

} // namespace store
} // namespace broker

#endif // BROKER_STORE_FRONTEND_HH
