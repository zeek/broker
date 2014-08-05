#ifndef BROKER_FACADE_HH
#define BROKER_FACADE_HH

#include <broker/data/types.hh>
#include <broker/Endpoint.hh>

#include <string>
#include <chrono>

namespace broker { namespace data {

class Facade {
public:

	Facade(const Endpoint& e, std::string topic);

	virtual ~Facade();

	const std::string& Topic() const;

	/*
	 * Update Interface - non-blocking.
	 * Changes may not be immediately visible.
	 */

	void Insert(Key k, Val v) const;

	void Erase(Key k) const;

	void Clear() const;

	// TODO: increment/decrement

	/*
	 * Query Interface - blocking.
	 * May have high latency.  TODO: is the convienience worth potential danger?
	 */

	std::unique_ptr<Val> Lookup(Key k) const;

	bool HasKey(Key k) const;

	std::unordered_set<Key> Keys() const;

	uint64_t Size() const;

	/*
	 * Query Interface - non-blocking.
	 */

	void Lookup(Key k, std::chrono::duration<double> timeout,
	            LookupCallback cb, void* cookie = nullptr) const;

	void HasKey(Key k, std::chrono::duration<double> timeout,
	            HasKeyCallback cb, void* cookie = nullptr) const;

	void Keys(std::chrono::duration<double> timeout,
	          KeysCallback cb, void* cookie = nullptr) const;

	void Size(std::chrono::duration<double> timeout,
	          SizeCallback cb, void* cookie = nullptr) const;

private:

	virtual void* GetBackendHandle() const;

	class Impl;
	std::unique_ptr<Impl> p;
};

} // namespace data
} // namespace broker

#endif // BROKER_FACADE_HH
