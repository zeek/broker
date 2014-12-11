#ifndef BROKER_ENUM_VALUE_HH
#define BROKER_ENUM_VALUE_HH

#include <broker/util/operators.hh>
#include <string>
#include <functional>

namespace broker {

/**
 * Stores the name of an enum value.  The receiver is responsible for knowing
 * how to map the name to the actual value if it needs that information.
 */
struct enum_value : util::totally_ordered<enum_value> {

	/**
	 * Default construct empty enum value name.
	 */
	enum_value()
		{}

	/**
	 * Construct enum value from a string.
	 */
	enum_value(std::string arg_name)
		: name(arg_name)
		{}

	std::string name;
};

inline bool operator==(const enum_value& lhs, const enum_value& rhs)
	{ return lhs.name == rhs.name; }

inline bool operator<(const enum_value& lhs, const enum_value& rhs)
	{ return lhs.name < rhs.name; }

} // namespace broker

namespace std {
template <> struct hash<broker::enum_value> {
	size_t operator()(const broker::enum_value& v) const
		{ return std::hash<std::string>{}(v.name); }
};
} // namespace std;

#endif // BROKER_ENUM_VALUE_HH
