#ifndef BROKER_FRONTEND_HH
#define BROKER_FRONTEND_HH

#include <broker/data/types.hh>
#include <broker/data/response_queue.hh>
#include <broker/endpoint.hh>
#include <string>
#include <chrono>

namespace broker { namespace data {

class frontend {
public:

	frontend(const endpoint& e, std::string topic);

	virtual ~frontend();

	const std::string& topic() const;

	response_queue responses() const;

	/*
	 * Update Interface - non-blocking.
	 * Changes may not be immediately visible.
	 */

	void insert(key k, value v) const;

	void erase(key k) const;

	void clear() const;

	// TODO: increment/decrement

	/*
	 * Query Interface - blocking.
	 * May have high latency.
	 */

	result request(query q) const;

	result lookup(key k) const
		{ return request(query(query::type::lookup, std::move(k))); }

	result exists(key k) const
		{ return request(query(query::type::exists, std::move(k))); }

	result keys() const
		{ return request(query(query::type::keys)); }

	result size() const
		{ return request(query(query::type::size)); }

	/*
	 * Query Interface - non-blocking.
	 */

	void request(query q, std::chrono::duration<double> timeout,
	             void* cookie = nullptr) const;

	void lookup(key k, std::chrono::duration<double> timeout,
	            void* cookie = nullptr) const
		{ request(query(query::type::lookup, std::move(k)), timeout, cookie); }

	void exists(key k, std::chrono::duration<double> timeout,
	             void* cookie = nullptr) const
		{ request(query(query::type::exists, std::move(k)), timeout, cookie); }

	void keys(std::chrono::duration<double> timeout,
	          void* cookie = nullptr) const
		{ request(query(query::type::keys), timeout, cookie); }

	void size(std::chrono::duration<double> timeout,
	          void* cookie = nullptr) const
		{ request(query(query::type::size), timeout, cookie); }

private:

	virtual void* handle() const;

	class impl;
	std::shared_ptr<impl> pimpl;
};

template <typename T>
std::unique_ptr<value> lookup(const T& f, key k)
	{
	result r = f.lookup(std::move(k));

	if ( r.stat != result::status::success )
		return {};

	if ( r.tag != result::type::value_val )
		return {};

	return std::unique_ptr<value>(new value(std::move(r.val)));
	}

template <typename T>
bool exists(const T& f, key k)
	{
	result r = f.exists(std::move(k));

	if ( r.stat != result::status::success )
		return false;

	return r.exists;
	}

template <typename T>
std::unordered_set<key> keys(const T& f)
	{
	result r = f.keys();

	if ( r.stat != result::status::success )
		return {};

	return std::move(r.keys);
	}

template <typename T>
uint64_t size(const T& f)
	{
	result r = f.size();

	if ( r.stat != result::status::success )
		return 0;

	return r.size;
	}

} // namespace data
} // namespace broker

#endif // BROKER_FRONTEND_HH
