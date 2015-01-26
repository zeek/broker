#ifndef BROKER_TIME_POINT_HH
#define BROKER_TIME_POINT_HH

#include <broker/util/operators.hh>
#include <broker/time_duration.hh>
#include <functional>
#include <ostream>

namespace broker {

/**
 * A point in time represented by a number of seconds from the Unix epoch.
 */
struct time_point : util::totally_ordered<time_point> {

	/**
	 * Construct a point in time equal to the Unix epoch (i.e. 0).
	 */
	time_point()
		: value(0)
		{}

	/**
	 * Construct a point in time that is some number of seconds away from the
	 * Unix epoch.
	 */
	explicit time_point(double seconds_from_epoch)
		: value(seconds_from_epoch)
		{}

	/**
	 * Construct a point in time that is some number of seconds away from the
	 * Unix epoch.
	 */
	time_point(const time_duration& seconds_from_epoch)
		: value(seconds_from_epoch.value)
		{}

	/**
	 * @return the current point in time.
	 */
	static time_point now();

	time_point& operator+=(const time_duration& rhs)
		{ value += rhs.value; return *this; }

	time_point& operator-=(const time_duration& rhs)
		{ value -= rhs.value; return *this; }

	double value;
};

inline bool operator==(const time_point& lhs, const time_point& rhs)
	{ return lhs.value == rhs.value; }

inline bool operator<(const time_point& lhs, const time_point& rhs)
	{ return lhs.value < rhs.value; }

inline time_point operator+(const time_point& lhs, const time_duration& rhs)
	{ return time_point{lhs.value + rhs.value}; }

inline time_point operator+(const time_duration& lhs, const time_point& rhs)
	{ return time_point{lhs.value + rhs.value}; }

inline time_point operator-(const time_point& lhs, const time_duration& rhs)
	{ return time_point{lhs.value - rhs.value}; }

inline std::ostream& operator<<(std::ostream& out, const time_point& d)
	{ return out << d.value; }

} // namespace broker

namespace std {
template <> struct hash<broker::time_point> {
	size_t operator()(const broker::time_point& v) const
		{ return std::hash<double>{}(v.value); }
};
} // namespace std;

#endif // BROKER_TIME_POINT_HH
