#ifndef BROKER_STORE_MEM_STORE_HH
#define BROKER_STORE_MEM_STORE_HH

#include <broker/store/store.hh>

namespace broker { namespace store {

/**
 * An in-memory implementation of a storage backend.
 */
class mem_store : public store {
public:

	/**
	 * Construct the in-memory storage from a data store snapshot.
	 */
	mem_store(snapshot sss = {})
	    : store(std::move(sss.sn)), datastore(std::move(sss.datastore))
		{ }

private:

	void do_insert(data k, data v, util::optional<expiration_time> t) override
		{ datastore[std::move(k)] = value{std::move(v), std::move(t)}; }

	void do_erase(const data& k) override
		{ datastore.erase(k); }

	void do_clear() override
		{ datastore.clear(); }

	util::optional<data> do_lookup(const data& k) const override
		{
		try { return datastore.at(k).item; }
		catch ( const std::out_of_range& ) { return {}; }
		}

	bool do_exists(const data& k) const override
		{
		if ( datastore.find(k) == datastore.end() ) return false;
		else return true;
		}

	std::unordered_set<data> do_keys() const override
		{
		std::unordered_set<data> rval;
		for ( const auto& kv : datastore ) rval.insert(kv.first);
		return rval;
		}

	uint64_t do_size() const override
		{ return datastore.size(); }

	snapshot do_snap() const override
		{ return {datastore, sequence()}; }

	std::deque<expirable> do_expiries() const override
		{
		std::deque<expirable> rval;

		for ( const auto& entry : datastore )
			if ( entry.second.expiry )
				rval.push_back({entry.first, *entry.second.expiry});

		return rval;
		}

	std::unordered_map<data, value> datastore;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_MEM_STORE_HH
