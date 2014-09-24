#ifndef BROKER_UTIL_HASH_HH
#define BROKER_UTIL_HASH_HH

#include <functional>

namespace broker {
namespace util {

template <typename T>
static inline void hash_combine(size_t& seed, const T& v)
	{ seed ^= std::hash<T>()(v) + 0x9e3779b9 + (seed<<6) + (seed>>2); }

template <typename C>
struct container_hasher {
	typedef typename C::value_type value_type;

	inline size_t operator()(const C& c) const
		{
		size_t rval = 0;
		for ( const auto& e : c )
			hash_combine<value_type>(rval, e);
		return rval;
		}
};

} // namespace util
} // namespace broker

#endif // BROKER_UTIL_HASH_HH
