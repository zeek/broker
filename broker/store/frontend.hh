#ifndef BROKER_STORE_FRONTEND_HH
#define BROKER_STORE_FRONTEND_HH

#include <broker/data.hh>
#include <broker/store/response_queue.hh>
#include <broker/util/optional.hh>
#include <broker/endpoint.hh>
#include <string>
#include <chrono>

namespace broker { namespace store {

class frontend {
public:

	frontend(const endpoint& e, std::string topic_name);

	virtual ~frontend();

	frontend(const frontend& other) = delete;

	frontend(frontend&& other);

	frontend& operator=(const frontend& other) = delete;

	frontend& operator=(frontend&& other);

	const std::string& topic_name() const;

	const response_queue& responses() const;

	/*
	 * Update Interface - non-blocking.
	 * Changes may not be immediately visible.
	 */

	void insert(data k, data v) const;

	void erase(data k) const;

	void clear() const;

	// TODO: increment/decrement

	/*
	 * Query Interface - blocking.
	 * May have high latency.
	 */

	result request(query q) const;

	result lookup(data k) const
		{ return request(query(query::type::lookup, std::move(k))); }

	result exists(data k) const
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

	void lookup(data k, std::chrono::duration<double> timeout,
	            void* cookie = nullptr) const
		{ request(query(query::type::lookup, std::move(k)), timeout, cookie); }

	void exists(data k, std::chrono::duration<double> timeout,
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
	std::unique_ptr<impl> pimpl;
};

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

template <typename T>
bool exists(const T& f, data k)
	{
	result r = f.exists(std::move(k));

	if ( r.stat != result::status::success )
		return false;

	return *util::get<bool>(r.value);
	}

template <typename T>
std::unordered_set<data> keys(const T& f)
	{
	result r = f.keys();

	if ( r.stat != result::status::success )
		return {};

	return *util::get<std::unordered_set<data>>(r.value);
	}

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
