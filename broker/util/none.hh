#ifndef BROKER_UTIL_NONE_HH
#define BROKER_UTIL_NONE_HH

namespace broker {
namespace util {

/**
 *  A type representing a null value.
 */
struct none_type {
	inline explicit operator bool() const
		{ return false; }
};

/**
 * Instance of none_type.
 */
static constexpr none_type none = none_type{};

} // namespace util
} // namespace broker

#endif // BROKER_UTIL_NONE_HH
