#include "broker/port.hh"
#include "broker/util/hash.hh"
#include <tuple>
#include <sstream>
#include <type_traits>

broker::port::port()
	: num(0), proto(protocol::unknown)
	{}

broker::port::port(number_type n, protocol p)
	: num(n), proto(p)
	{}

broker::port::number_type broker::port::number() const
	{ return num; }

broker::port::protocol broker::port::type() const
	{ return proto; }

std::string broker::to_string(const port& p)
	{
	std::ostringstream oss;
	oss << p.num;

	switch ( p.proto ) {
	case port::protocol::unknown:
		oss << "unknown";
		break;
	case port::protocol::tcp:
		oss << "tcp";
		break;
	case port::protocol::udp:
		oss << "udp";
		break;
	case port::protocol::icmp:
		oss << "icmp";
		break;
	default:
		oss << "?";
		break;
	}

	return oss.str();
	}

std::ostream& broker::operator<<(std::ostream& out, const port& p)
	{ return out << to_string(p); }

bool broker::operator==(const port& lhs, const port& rhs)
	{ return lhs.proto == rhs.proto && lhs.num == rhs.num; }

bool broker::operator<(const port& lhs, const port& rhs)
	{ return std::tie(lhs.num, lhs.proto) < std::tie(rhs.num, rhs.proto); }

size_t std::hash<broker::port>::operator()(const broker::port& v) const
	{
	using broker::port;
	size_t rval = 0;
	broker::util::hash_combine(rval, v.number());
	auto p = static_cast<std::underlying_type<port::protocol>::type>(v.type());
	broker::util::hash_combine(rval, p);
	return rval;
	}
