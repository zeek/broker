#ifndef BROKER_TOPIC_HH
#define BROKER_TOPIC_HH

#include <cstdint>
#include <type_traits>
#include <string>

namespace broker {

/**
 * A topic string used for broker's supported communication patterns.
 * Print, event, and log topics use a pub/sub communication pattern
 * that uses the topic name as a prefix to match when determining
 * subscribers of a given topic.
 */
struct topic {

	/**
	 * A distinguishing tag for broker's various messaging patterns.
	 * Topic names with one tag have no association with the same topic
	 * names with another tag.
	 */
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

/**
 * @return the integral value associated with the given topic tag.
 */
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
