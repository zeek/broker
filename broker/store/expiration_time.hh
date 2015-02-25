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
	 * A time or interval measured in seconds, depending on which type of expiry
	 * method is used.
	 */
	double expiry_time;

	/**
	 * A last-modified time, not used by the absolute time expiry method.
	 */
	double modification_time;

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
	 * @param arg_expiry_time interval from the last modification time at
	 * which to expire.
	 * @param arg_modification_time the last modification time.
	 */
	expiration_time(double arg_expiry_time, double arg_modification_time)
		: expiry_time(arg_expiry_time),
	      modification_time(arg_modification_time),
	      type(tag::since_last_modification)
		{}

	/**
	 * Construct expiration at an absolute point in time.
	 * @param arg_expiry_time an absolute time (seconds from epoch) at which
	 * to expire.
	 */
	expiration_time(double arg_expiry_time)
		: expiry_time(arg_expiry_time),
	      modification_time(),
	      type(tag::absolute)
		{}
};

inline bool operator==(const expiration_time& lhs, const expiration_time& rhs)
    {
	if ( lhs.type != rhs.type )
		return false;

	if ( lhs.expiry_time != rhs.expiry_time )
		return false;

	if ( lhs.type == expiration_time::tag::since_last_modification )
		return lhs.modification_time == rhs.modification_time;

	return true;
	}

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
