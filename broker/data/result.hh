#ifndef BROKER_DATA_RESULT_HH
#define BROKER_DATA_RESULT_HH

#include <broker/data/types.hh>
#include <broker/data/snapshot.hh>
#include <cstdint>
#include <unordered_set>

namespace broker { namespace data {

class result {
public:

	// This tag indicates which value of the union is currently valid.
	// For statuses other than success, it's arbitrarily tagged w/ exists_val.
	enum class type: uint8_t {
		exists_val,
		size_val,
		value_val,
		keys_val,
		snapshot_val
	} tag;

	enum class status : uint8_t {
		success,
		failure,  // Query could not be fulfilled.
		timeout
	} stat;

	union {
		bool exists;
		uint64_t size;
		value val;
		std::unordered_set<key> keys;
		snapshot snap;
	};

	result()
	    : tag(type::exists_val), stat(status::failure), exists()
		{ }

	result(status s)
		: tag(type::exists_val), stat(s), exists()
		{ }

	explicit result(bool bval)
	    : tag(type::exists_val), stat(status::success), exists(bval)
		{ }

	explicit result(uint64_t sz)
		: tag(type::size_val), stat(status::success), size(sz)
		{ }

	explicit result(value v)
		: tag(type::value_val), stat(status::success), val(std::move(v))
		{ }

	explicit result(std::unique_ptr<value> v)
		: tag(v ? type::value_val : type::exists_val), stat(status::success)
		{
		if ( tag == type::value_val )
			new (&val) value(std::move(*v.get()));
		else
			new (&exists) bool(false);
		}

	explicit result(std::unordered_set<key> ks)
		: tag(type::keys_val), stat(status::success), keys(std::move(ks))
		{ }

	explicit result(snapshot sss)
		: tag(type::snapshot_val), stat(status::success),
	      snap(std::move(sss))
		{ }

	result(const result& other)
		{
		copy(other);
		}

	result(result&& other)
		{
		steal(std::move(other));
		}

	result& operator=(const result& other)
		{
		if ( this == &other ) return *this;
		clear();
		copy(other);
		return *this;
		}

	result& operator=(result&& other)
		{
		clear();
		steal(std::move(other));
		return *this;
		}

	~result()
		{
		clear();
		}

private:

	void clear()
		{
		using namespace std;
		switch ( tag ) {
		case result::type::value_val:
			val.~value();
			break;
		case result::type::keys_val:
			keys.~unordered_set<key>();
			break;
		case result::type::snapshot_val:
			snap.~snapshot();
		default:
			break;
		}
		}

	void copy(const result& other)
		{
		tag = other.tag;
		stat = other.stat;
		switch ( tag ) {
		case result::type::exists_val:
			new (&exists) bool(other.exists);
			break;
		case result::type::size_val:
			new (&size) uint64_t(other.size);
			break;
		case result::type::value_val:
			new (&val) value(other.val);
			break;
		case result::type::keys_val:
			new (&keys) std::unordered_set<key>(other.keys);
			break;
		case result::type::snapshot_val:
			new (&snap) snapshot(other.snap);
			break;
		}
		}

	void steal(result&& other)
		{
		tag = other.tag;
		stat = other.stat;
		switch ( tag ) {
		case result::type::exists_val:
			new (&exists) bool(other.exists);
			break;
		case result::type::size_val:
			new (&size) uint64_t(other.size);
			break;
		case result::type::value_val:
			new (&val) value(std::move(other.val));
			break;
		case result::type::keys_val:
			new (&keys) std::unordered_set<key>(std::move(other.keys));
			break;
		case result::type::snapshot_val:
			new (&snap) snapshot(std::move(other.snap));
			break;
		}
		}
};

inline bool operator==(const result& lhs, const result& rhs)
    {
	if ( lhs.tag != rhs.tag ) return false;
	if ( lhs.stat != rhs.stat ) return false;
	switch ( lhs.tag ) {
	case result::type::value_val:
		return lhs.val == rhs.val;
	case result::type::exists_val:
		return lhs.exists == rhs.exists;
	case result::type::keys_val:
		return lhs.keys == rhs.keys;
	case result::type::size_val:
		return lhs.size == rhs.size;
	case result::type::snapshot_val:
		return lhs.snap == rhs.snap;
	}
	}

} // namespace data
} // namespace broker

#endif // BROKER_DATA_RESULT_HH
