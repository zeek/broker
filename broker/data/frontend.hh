#ifndef BROKER_FRONTEND_HH
#define BROKER_FRONTEND_HH

#include <broker/data/types.hh>
#include <broker/endpoint.hh>
#include <string>
#include <chrono>

namespace broker { namespace data {

class frontend {
public:

	frontend(const endpoint& e, std::string topic);

	virtual ~frontend();

	const std::string& topic() const;

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

	std::unique_ptr<value> lookup(key k) const;

	bool has_key(key k) const;

	std::unordered_set<key> keys() const;

	uint64_t size() const;

	/*
	 * Query Interface - non-blocking.
	 */

	void lookup(key k, std::chrono::duration<double> timeout,
	            LookupCallback cb, void* cookie = nullptr) const;

	void has_key(key k, std::chrono::duration<double> timeout,
	             HasKeyCallback cb, void* cookie = nullptr) const;

	void keys(std::chrono::duration<double> timeout,
	          KeysCallback cb, void* cookie = nullptr) const;

	void size(std::chrono::duration<double> timeout,
	          SizeCallback cb, void* cookie = nullptr) const;

private:

	virtual void* handle() const;

	class impl;
	std::shared_ptr<impl> pimpl;
};

} // namespace data
} // namespace broker

#endif // BROKER_FRONTEND_HH
