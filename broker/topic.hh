#ifndef BROKER_TOPIC_HH
#define BROKER_TOPIC_HH

#include <cstdint>
#include <type_traits>
#include <string>

namespace broker {

struct topic {

	enum class tag : uint16_t {
		print,
		event,
		log,
		store_query,
		store_update,
		last          // Sentinel for last enum value.
	};

	std::string name;
	tag type;
};

constexpr std::underlying_type<topic::tag>::type operator+(topic::tag t)
	{ return static_cast<std::underlying_type<topic::tag>::type>(t); }

inline bool operator==(const topic& lhs, const topic& rhs)
	{ return lhs.type == rhs.type && lhs.name == rhs.name; }
inline bool operator!=(const topic& lhs, const topic& rhs)
	{ return ! operator==(lhs,rhs); }
inline bool operator<(const topic& lhs, const topic& rhs)
	{ return lhs.name < rhs.name || ( ! (rhs.name < lhs.name) &&
	                                  lhs.type < rhs.type ); }
inline bool operator>(const topic& lhs, const topic& rhs)
	{ return operator<(rhs,lhs); }
inline bool operator<=(const topic& lhs, const topic& rhs)
	{ return ! operator>(lhs,rhs); }
inline bool operator>=(const topic& lhs, const topic& rhs)
	{ return ! operator<(lhs,rhs); }

} // namespace broker

#endif // BROKER_TOPIC_HH
