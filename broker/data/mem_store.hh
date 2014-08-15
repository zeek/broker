#ifndef BROKER_DATA_MEM_STORE_HH
#define BROKER_DATA_MEM_STORE_HH

#include <broker/data/store.hh>

namespace broker { namespace data {

class mem_store : public store {
public:

	mem_store(snapshot sss = {})
	    : store(std::move(sss.sn)), datastore(std::move(sss.datastore))
		{ }

private:

	void do_insert(key k, value v) override
		{ datastore[std::move(k)] = std::move(v); }

	void do_erase(const key& k) override
		{ datastore.erase(k); }

	void do_clear() override
		{ datastore.clear(); }

	std::unique_ptr<value> do_lookup(const key& k) const override
		{
		try { return std::unique_ptr<value>(new value(datastore.at(k))); }
		catch ( const std::out_of_range& ) { return {}; }
		}

	bool do_exists(const key& k) const override
		{
		if ( datastore.find(k) == datastore.end() ) return false;
		else return true;
		}

	std::unordered_set<key> do_keys() const override
		{
		std::unordered_set<key> rval;
		for ( const auto& kv : datastore ) rval.insert(kv.first);
		return rval;
		}

	uint64_t do_size() const override
		{ return datastore.size(); }

	snapshot do_snap() const override
		{ return {datastore, sequence()}; }

	std::unordered_map<key, value> datastore;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_MEM_STORE_HH
