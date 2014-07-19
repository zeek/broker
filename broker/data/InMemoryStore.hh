#ifndef BROKER_DATA_INMEMORYSTORE_HH
#define BROKER_DATA_INMEMORYSTORE_HH

#include <broker/data/Store.hh>

#include <unordered_map>

namespace broker { namespace data {

class InMemoryStore : public Store {
private:

	void DoInsert(Key k, Val v) override
		{ store.insert(std::make_pair<Key, Val>(std::move(k), std::move(v))); }

	void DoErase(Key k) override
		{ store.erase(k); }

	void DoClear() override
		{ store.clear(); }

	std::unique_ptr<Val> DoLookup(Key k) override
		{
		try { return std::unique_ptr<Val>(new Val(store.at(k))); }
		catch ( const std::out_of_range& ) { return {}; }
		}

	bool DoHasKey(Key k) override
		{ if ( store.find(k) == store.end() ) return false; else return true; }

	std::unordered_set<Key> DoKeys() override
		{
		std::unordered_set<Key> rval;
		for ( const auto& kv : store ) rval.insert(kv.first);
		return rval;
		}

	uint64_t DoSize() override
		{ return store.size(); }

	std::unordered_map<Key, Val> store;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_INMEMORYSTORE_HH
