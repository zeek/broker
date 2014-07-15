#ifndef BROKER_FACADE_HH
#define BROKER_FACADE_HH

#include <broker/data/types.hh>
#include <broker/Endpoint.hh>

#include <string>

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

	/*
	 * Query Interface - blocking.
	 * May have high latency unless Facade subclass guarantees a local copy
	 * of all data.
	 */

	std::unique_ptr<Val> Lookup(Key k) const;

	std::unordered_set<Key> Keys() const;

	uint64_t Size() const;

	/*
	 * Query Interface - non-blocking.
	 */

	void Lookup(Key k, LookupCallback cb, void* cookie) const;

	void Keys(KeysCallback cb, void* cookie) const;

	void Size(SizeCallback cb, void* cookie) const;

private:

	class Impl;
	std::unique_ptr<Impl> p;
};

} // namespace data
} // namespace broker

#endif // BROKER_FACADE_HH
