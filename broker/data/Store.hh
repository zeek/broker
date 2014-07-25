#ifndef BROKER_DATA_STORE_HH
#define BROKER_DATA_STORE_HH

#include <broker/data/types.hh>
#include <broker/data/SequenceNum.hh>

#include <unordered_map>

namespace broker { namespace data {

class StoreSnapshot {
public:

	std::unordered_map<Key, Val> store;
	SequenceNum sn;
};

inline bool operator==(const StoreSnapshot& lhs, const StoreSnapshot& rhs)
    { return lhs.sn == rhs.sn && lhs.store == rhs.store; }

class Store {
public:

	Store(SequenceNum arg_sn = {}) : sn(std::move(arg_sn)) { }

	virtual ~Store() { }

	const SequenceNum& GetSequenceNum() const
		{ return sn; }

	void Insert(Key k, Val v)
		{ ++sn; DoInsert(std::move(k), std::move(v)); }

	// TODO: increment/decrement

	void Erase(const Key& k)
		{ ++sn; DoErase(k); }

	void Clear()
		{ ++sn; DoClear(); }

	std::unique_ptr<Val> Lookup(const Key& k) const
		{ return DoLookup(k); }

	bool HasKey(const Key& k) const
		{ return DoHasKey(k); }

	std::unordered_set<Key> Keys() const
		{ return DoKeys(); }

	uint64_t Size() const
		{ return DoSize(); }

	StoreSnapshot Snapshot() const
		{ return DoSnapshot(); }

private:

	virtual void DoInsert(Key k, Val v) = 0;

	virtual void DoErase(const Key& k) = 0;

	virtual void DoClear() = 0;

	virtual std::unique_ptr<Val> DoLookup(const Key& k) const = 0;

	virtual bool DoHasKey(const Key& k) const = 0;

	virtual std::unordered_set<Key> DoKeys() const = 0;

	virtual uint64_t DoSize() const = 0;

	virtual StoreSnapshot DoSnapshot() const = 0;

	SequenceNum sn;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_STORE_HH
