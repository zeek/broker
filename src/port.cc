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

// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_port* broker_port_create()
	{ return reinterpret_cast<broker_port*>(new (nothrow) broker::port()); }

void broker_port_delete(broker_port* p)
	{ delete reinterpret_cast<broker::port*>(p); }

broker_port* broker_port_copy(const broker_port* p)
	{
	auto pp = reinterpret_cast<const broker::port*>(p);
	return reinterpret_cast<broker_port*>(new (nothrow) broker::port(*pp));
	}

broker_port* broker_port_from(uint16_t num, broker_port_protocol p)
	{
	auto rval = new (nothrow)
	            broker::port(num, static_cast<broker::port::protocol>(p));
	return reinterpret_cast<broker_port*>(rval);
	}

void broker_port_set_number(broker_port* dst, uint16_t num)
	{
	auto d = reinterpret_cast<broker::port*>(dst);
	*d = broker::port(num, d->type());
	}

void broker_port_set_protocol(broker_port* dst, broker_port_protocol p)
	{
	auto d = reinterpret_cast<broker::port*>(dst);
	*d = broker::port(d->number(), static_cast<broker::port::protocol>(p));
	}

uint16_t broker_port_number(const broker_port* p)
	{ return reinterpret_cast<const broker::port*>(p)->number(); }

broker_port_protocol broker_port_type(const broker_port* p)
	{
	auto pp = reinterpret_cast<const broker::port*>(p);
	return static_cast<broker_port_protocol>(pp->type());
	}

broker_string* broker_port_to_string(const broker_port* p)
	{
	auto pp = reinterpret_cast<const broker::port*>(p);

	try
		{
		auto rval = broker::to_string(*pp);
		return reinterpret_cast<broker_string*>(
		            new std::string(std::move(rval)));
		}
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

int broker_port_eq(const broker_port* a, const broker_port* b)
	{
	auto aa = reinterpret_cast<const broker::port*>(a);
	auto bb = reinterpret_cast<const broker::port*>(b);
	return *aa == *bb;
	}

int broker_port_lt(const broker_port* a, const broker_port* b)
	{
	auto aa = reinterpret_cast<const broker::port*>(a);
	auto bb = reinterpret_cast<const broker::port*>(b);
	return *aa < *bb;
	}

size_t broker_port_hash(const broker_port* p)
	{
	auto pp = reinterpret_cast<const broker::port*>(p);
	return std::hash<broker::port>{}(*pp);
	}
