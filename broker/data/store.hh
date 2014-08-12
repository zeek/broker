#ifndef BROKER_DATA_STORE_HH
#define BROKER_DATA_STORE_HH

#include <broker/data/types.hh>
#include <broker/data/sequence_num.hh>
#include <unordered_map>

namespace broker { namespace data {

class store_snapshot {
public:

	std::unordered_map<key, value> datastore;
	sequence_num sn;
};

inline bool operator==(const store_snapshot& lhs, const store_snapshot& rhs)
    { return lhs.sn == rhs.sn && lhs.datastore == rhs.datastore; }

class store {
public:

	store(sequence_num arg_sn = {}) : sn(std::move(arg_sn)) { }

	virtual ~store() { }

	const sequence_num& sequence() const
		{ return sn; }

	void insert(key k, value v)
		{ ++sn; do_insert(std::move(k), std::move(v)); }

	// TODO: increment/decrement

	void erase(const key& k)
		{ ++sn; do_erase(k); }

	void clear()
		{ ++sn; do_clear(); }

	std::unique_ptr<value> lookup(const key& k) const
		{ return do_lookup(k); }

	bool has_key(const key& k) const
		{ return do_has_key(k); }

	std::unordered_set<key> keys() const
		{ return do_keys(); }

	uint64_t size() const
		{ return do_size(); }

	store_snapshot snapshot() const
		{ return do_snapshot(); }

private:

	virtual void do_insert(key k, value v) = 0;

	virtual void do_erase(const key& k) = 0;

	virtual void do_clear() = 0;

	virtual std::unique_ptr<value> do_lookup(const key& k) const = 0;

	virtual bool do_has_key(const key& k) const = 0;

	virtual std::unordered_set<key> do_keys() const = 0;

	virtual uint64_t do_size() const = 0;

	virtual store_snapshot do_snapshot() const = 0;

	sequence_num sn;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_STORE_HH
