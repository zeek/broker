#ifndef BROKER_STORE_STORE_HH
#define BROKER_STORE_STORE_HH

#include <broker/data.hh>
#include <broker/store/sequence_num.hh>
#include <broker/store/snapshot.hh>
#include <broker/util/optional.hh>
#include <unordered_map>
#include <unordered_set>

namespace broker { namespace store {

class store {
public:

	store(sequence_num arg_sn = {}) : sn(std::move(arg_sn)) { }

	virtual ~store() { }

	const sequence_num& sequence() const
		{ return sn; }

	void insert(data k, data v)
		{ ++sn; do_insert(std::move(k), std::move(v)); }

	// TODO: increment/decrement

	void erase(const data& k)
		{ ++sn; do_erase(k); }

	void clear()
		{ ++sn; do_clear(); }

	util::optional<data> lookup(const data& k) const
		{ return do_lookup(k); }

	bool exists(const data& k) const
		{ return do_exists(k); }

	std::unordered_set<data> keys() const
		{ return do_keys(); }

	uint64_t size() const
		{ return do_size(); }

	snapshot snap() const
		{ return do_snap(); }

private:

	virtual void do_insert(data k, data v) = 0;

	virtual void do_erase(const data& k) = 0;

	virtual void do_clear() = 0;

	virtual util::optional<data> do_lookup(const data& k) const = 0;

	virtual bool do_exists(const data& k) const = 0;

	virtual std::unordered_set<data> do_keys() const = 0;

	virtual uint64_t do_size() const = 0;

	virtual snapshot do_snap() const = 0;

	sequence_num sn;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_STORE_HH
