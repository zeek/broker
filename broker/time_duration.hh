#ifndef BROKER_TIME_DURATION_HH
#define BROKER_TIME_DURATION_HH

#include <broker/util/operators.hh>
#include <functional>
#include <ostream>

namespace broker {

/**
 * A duration of time, measured in seconds.
 */
struct time_duration : util::totally_ordered<time_duration> {

	/**
	 * Construct an empty duration (i.e. 0 seconds).
	 */
	time_duration()
		: value(0)
		{}

	/**
	 * Construct a duration from a given number of seconds.
	 */
	explicit time_duration(double seconds)
		: value(seconds)
		{}

	time_duration& operator+=(const time_duration& rhs)
		{ value += rhs.value; return *this; }

	time_duration& operator-=(const time_duration& rhs)
		{ value -= rhs.value; return *this; }

	time_duration& operator*=(double rhs)
		{ value *= rhs; return *this; }

	time_duration& operator/=(double rhs)
		{ value /= rhs; return *this; }

	double value;
};

inline bool operator==(const time_duration& lhs, const time_duration& rhs)
	{ return lhs.value == rhs.value; }

inline bool operator<(const time_duration& lhs, const time_duration& rhs)
	{ return lhs.value < rhs.value; }

inline time_duration operator+(const time_duration& lhs,
                               const time_duration& rhs)
	{ return time_duration{lhs.value + rhs.value}; }

inline time_duration operator-(const time_duration& lhs,
                               const time_duration& rhs)
	{ return time_duration{lhs.value - rhs.value}; }

inline time_duration operator*(const time_duration& lhs, double rhs)
	{ return time_duration{lhs.value * rhs}; }

inline time_duration operator/(const time_duration& lhs, double rhs)
	{ return time_duration{lhs.value / rhs}; }

inline std::ostream& operator<<(std::ostream& out, const time_duration& d)
	{ return out << d.value; }

} // namespace broker

namespace std {
template <> struct hash<broker::time_duration> {
	size_t operator()(const broker::time_duration& v) const
		{ return std::hash<double>{}(v.value); }
};
} // namespace std;

#endif // BROKER_TIME_DURATION_HH
