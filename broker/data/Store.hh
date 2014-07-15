#ifndef BROKER_DATA_STORE_HH
#define BROKER_DATA_STORE_HH

#include <broker/data/types.hh>

namespace broker { namespace data {

class Store {
public:

	virtual ~Store() { }

	void Insert(Key k, Val v)
		{ DoInsert(std::move(k), std::move(v)); }

	void Erase(Key k)
		{ DoErase(std::move(k)); }

	void Clear()
		{ DoClear(); }

	std::unique_ptr<Val> Lookup(Key k)
		{ return DoLookup(std::move(k)); }

	bool HasKey(Key k)
		{ return DoHasKey(std::move(k)); }

	std::unordered_set<Key> Keys()
		{ return DoKeys(); }

	uint64_t Size()
		{ return DoSize(); }

private:

	virtual void DoInsert(Key k, Val v) = 0;

	virtual void DoErase(Key k) = 0;

	virtual void DoClear() = 0;

	virtual std::unique_ptr<Val> DoLookup(Key k) = 0;

	virtual bool DoHasKey(Key k) = 0;

	virtual std::unordered_set<Key> DoKeys() = 0;

	virtual uint64_t DoSize() = 0;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_STORE_HH
