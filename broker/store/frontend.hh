#ifndef BROKER_STORE_FRONTEND_HH
#define BROKER_STORE_FRONTEND_HH

#include <broker/data.hh>
#include <broker/store/response_queue.hh>
#include <broker/util/optional.hh>
#include <broker/endpoint.hh>
#include <string>
#include <chrono>

namespace broker { namespace store {

/**
 * A frontend interface of a data store (either a master or clone)
 * that allows querying and updating contents.
 */
class frontend {
public:

	/**
	 * Construct a data store frontend to a master data store.
	 * @param e the broker endpoint to attach the frontend.
	 * @param topic_name the exact topic name that a master data store is using.
	 * The master store must be attached either directly to the same endpoint as
	 * the to one of its peers.  If attached to a peer, the endpoint must
	 * allow advertising interest in this topic name.
	 */
	frontend(const endpoint& e, std::string topic_name);

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
	 * @return the topic name of the data store.
	 */
	const std::string& topic_name() const;

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
	 */
	void insert(data k, data v) const;

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

	// TODO: increment/decrement

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
		{ return request(query(query::type::lookup, std::move(k))); }

	/**
	 * Make a query and block until response is received.
	 * May have high latency if data is non-local.
	 * @param k the key to check for existence.
	 * @return the result of the query.
	 */
	result exists(data k) const
		{ return request(query(query::type::exists, std::move(k))); }

	/**
	 * Make a query and block until response is received.
	 * May have high latency if data is non-local.
	 * @return the result of the query -- all keys in the data store.
	 */
	result keys() const
		{ return request(query(query::type::keys)); }

	/**
	 * Make a query and block until response is received.
	 * May have high latency if data is non-local.
	 * @return the result of the query -- the number of key-value pairs in the
	 * data store.
	 */
	result size() const
		{ return request(query(query::type::size)); }

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
		{ request(query(query::type::lookup, std::move(k)), timeout, cookie); }

	/**
	 * Make a non-blocking query to check for a key's existence..
	 * @param k the key to check for existence.
	 * @param timeout the amount of time after which the query times out.
	 * @param cookie a pointer value to make available in the result/response
	 * when it is available.
	 */
	void exists(data k, std::chrono::duration<double> timeout,
	             void* cookie = nullptr) const
		{ request(query(query::type::exists, std::move(k)), timeout, cookie); }

	/**
	 * Make a non-blocking query to obtain all keys in the store.
	 * @param timeout the amount of time after which the query times out.
	 * @param cookie a pointer value to make available in the result/response
	 * when it is available.
	 */
	void keys(std::chrono::duration<double> timeout,
	          void* cookie = nullptr) const
		{ request(query(query::type::keys), timeout, cookie); }

	/**
	 * Make a non-blocking query to obtain the number of key-value pairs in the
	 * store.
	 * @param timeout the amount of time after which the query times out.
	 * @param cookie a pointer value to make available in the result/response
	 * when it is available.
	 */
	void size(std::chrono::duration<double> timeout,
	          void* cookie = nullptr) const
		{ request(query(query::type::size), timeout, cookie); }

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
std::unordered_set<data> keys(const T& f)
	{
	result r = f.keys();

	if ( r.stat != result::status::success )
		return {};

	return *util::get<std::unordered_set<data>>(r.value);
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
