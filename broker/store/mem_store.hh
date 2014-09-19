#ifndef BROKER_STORE_MEM_STORE_HH
#define BROKER_STORE_MEM_STORE_HH

#include <broker/store/store.hh>

namespace broker { namespace store {

class mem_store : public store {
public:

	mem_store(snapshot sss = {})
	    : store(std::move(sss.sn)), datastore(std::move(sss.datastore))
		{ }

private:

	void do_insert(data k, data v) override
		{ datastore[std::move(k)] = std::move(v); }

	void do_erase(const data& k) override
		{ datastore.erase(k); }

	void do_clear() override
		{ datastore.clear(); }

	std::unique_ptr<data> do_lookup(const data& k) const override
		{
		try { return std::unique_ptr<data>(new data(datastore.at(k))); }
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

	std::unordered_map<data, data> datastore;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_MEM_STORE_HH
