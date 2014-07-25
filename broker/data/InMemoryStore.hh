#ifndef BROKER_DATA_INMEMORYSTORE_HH
#define BROKER_DATA_INMEMORYSTORE_HH

#include <broker/data/Store.hh>

namespace broker { namespace data {

class InMemoryStore : public Store {
public:

	InMemoryStore(StoreSnapshot sss = {})
	    : Store(std::move(sss.sn)), store(std::move(sss.store))
		{ }

private:

	void DoInsert(Key k, Val v) override
		{ store[std::move(k)] = std::move(v); }

	void DoErase(const Key& k) override
		{ store.erase(k); }

	void DoClear() override
		{ store.clear(); }

	std::unique_ptr<Val> DoLookup(const Key& k) const override
		{
		try { return std::unique_ptr<Val>(new Val(store.at(k))); }
		catch ( const std::out_of_range& ) { return {}; }
		}

	bool DoHasKey(const Key& k) const override
		{ if ( store.find(k) == store.end() ) return false; else return true; }

	std::unordered_set<Key> DoKeys() const override
		{
		std::unordered_set<Key> rval;
		for ( const auto& kv : store ) rval.insert(kv.first);
		return rval;
		}

	uint64_t DoSize() const override
		{ return store.size(); }

	StoreSnapshot DoSnapshot() const override
		{ return {store, GetSequenceNum()}; }

	std::unordered_map<Key, Val> store;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_INMEMORYSTORE_HH
