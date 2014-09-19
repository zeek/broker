#ifndef BROKER_STORE_STORE_HH
#define BROKER_STORE_STORE_HH

#include <broker/store/types.hh>
#include <broker/store/sequence_num.hh>
#include <broker/store/snapshot.hh>
#include <unordered_map>
#include <unordered_set>

namespace broker { namespace store {

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

	bool exists(const key& k) const
		{ return do_exists(k); }

	std::unordered_set<key> keys() const
		{ return do_keys(); }

	uint64_t size() const
		{ return do_size(); }

	snapshot snap() const
		{ return do_snap(); }

private:

	virtual void do_insert(key k, value v) = 0;

	virtual void do_erase(const key& k) = 0;

	virtual void do_clear() = 0;

	virtual std::unique_ptr<value> do_lookup(const key& k) const = 0;

	virtual bool do_exists(const key& k) const = 0;

	virtual std::unordered_set<key> do_keys() const = 0;

	virtual uint64_t do_size() const = 0;

	virtual snapshot do_snap() const = 0;

	sequence_num sn;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_STORE_HH
