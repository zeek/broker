#ifndef BROKER_STORE_EXPIRATION_TIME_HH
#define BROKER_STORE_EXPIRATION_TIME_HH

#include <broker/data.hh>
#include <broker/util/optional.hh>

namespace broker {
namespace store {

/**
 * Represents how to expire a given entry in a data store.
 */
class expiration_time {
public:

	/**
	 * A time or interval measured in seconds.
	 */
	double time;

	/**
	 * Differentiates different styles of expiration.
	 */
	enum class tag : uint8_t {
		since_last_modification, // seconds since value was last modified
		absolute,                // seconds since Jan. 1, 1970.
	} type;

	/**
	 * Default ctor.
	 */
	expiration_time() = default;

	/**
	 * Construct expiration relative to a last modification time.
	 */
	expiration_time(double arg_time,
	                tag arg_type = tag::since_last_modification)
		: time(arg_time), type(arg_type)
		{}
};

inline bool operator==(const expiration_time& lhs, const expiration_time& rhs)
    { return lhs.type == rhs.type && lhs.time == rhs.time; }

/**
 * A key in a data store which has a given expiration.
 */
struct expirable {
	data key;
	expiration_time expiry;
};

inline bool operator==(const expirable& lhs, const expirable& rhs)
    { return lhs.key == rhs.key && lhs.expiry == rhs.expiry; }

} // namespace store
} // namespace broker

#endif // BROKER_STORE_EXPIRATION_TIME_HH
