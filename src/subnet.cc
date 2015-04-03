#include "broker/subnet.hh"
#include "broker/util/hash.hh"
#include <tuple>
#include <sstream>

broker::subnet::subnet()
	: len(0)
	{}

broker::subnet::subnet(address addr, uint8_t length)
	: net(std::move(addr)), len(length)
	{
	if ( init() ) return;
	net = {};
	len = 0;
	}

bool broker::subnet::init()
	{
	if ( net.is_v4() )
		{
		if ( len > 32 )
			return false;

		len += 96;
		}
	else if ( len > 128 )
		{
		return false;
		}

	net.mask(len);
	return true;
	}

bool broker::subnet::contains(const address& addr) const
	{
	address p{addr};
	p.mask(len);
	return p == net;
	}

const broker::address& broker::subnet::network() const
	{ return net; }

uint8_t broker::subnet::length() const
	{ return net.is_v4() ? len - 96 : len; }

std::string broker::to_string(const subnet& s)
	{
	std::ostringstream oss;
	oss << s.net << "/" << (s.net.is_v4() ? s.len - 96 : s.len);
	return oss.str();
	}

std::ostream& broker::operator<<(std::ostream& out, const broker::subnet& s)
	{ return out << to_string(s); }

bool broker::operator==(const subnet& lhs, const subnet& rhs)
	{ return lhs.len == rhs.len && lhs.net == rhs.net; }

bool broker::operator<(const subnet& lhs, const subnet& rhs)
	{ return std::tie(lhs.net, lhs.len) < std::tie(rhs.net, lhs.len); }

size_t std::hash<broker::subnet>::operator()(const broker::subnet& v) const
	{
	size_t rval = 0;
	broker::util::hash_combine(rval, v.network());
	broker::util::hash_combine(rval, v.length());
	return rval;
	}

// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_subnet* broker_subnet_create()
	{ return reinterpret_cast<broker_subnet*>(new (nothrow) broker::subnet()); }

void broker_subnet_delete(broker_subnet* s)
	{ delete reinterpret_cast<broker::subnet*>(s); }

broker_subnet* broker_subnet_copy(const broker_subnet* s)
	{
	auto ss = reinterpret_cast<const broker::subnet*>(s);
	return reinterpret_cast<broker_subnet*>(new (nothrow) broker::subnet(*ss));
	}

broker_subnet* broker_subnet_from(const broker_address* a, uint8_t len)
	{
	auto aa = reinterpret_cast<const broker::address*>(a);
	return reinterpret_cast<broker_subnet*>(
	            new (nothrow) broker::subnet(*aa, len));
	}

void broker_subnet_set(broker_subnet* dst, broker_subnet* src)
	{
	auto d = reinterpret_cast<broker::subnet*>(dst);
	auto s = reinterpret_cast<broker::subnet*>(src);
	*d = *s;
	}

int broker_subnet_contains(const broker_subnet* s, const broker_address* a)
	{
	auto aa = reinterpret_cast<const broker::address*>(a);
	return reinterpret_cast<const broker::subnet*>(s)->contains(*aa);
	}

const broker_address* broker_subnet_network(const broker_subnet* s)
	{
	auto ss = reinterpret_cast<const broker::subnet*>(s);
	return reinterpret_cast<const broker_address*>(&ss->network());
	}

uint8_t broker_subnet_length(const broker_subnet* s)
	{
	auto ss = reinterpret_cast<const broker::subnet*>(s);
	return ss->length();
	}

broker_string* broker_subnet_to_string(const broker_subnet* s)
	{
	auto ss = reinterpret_cast<const broker::subnet*>(s);

	try
		{
		auto rval = broker::to_string(*ss);
		return reinterpret_cast<broker_string*>(
		             new std::string(std::move(rval)));
		}
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

int broker_subnet_eq(const broker_subnet* a, const broker_subnet* b)
	{
	auto aa = reinterpret_cast<const broker::subnet*>(a);
	auto bb = reinterpret_cast<const broker::subnet*>(b);
	return *aa == *bb;
	}

int broker_subnet_lt(const broker_subnet* a, const broker_subnet* b)
	{
	auto aa = reinterpret_cast<const broker::subnet*>(a);
	auto bb = reinterpret_cast<const broker::subnet*>(b);
	return *aa < *bb;
	}

size_t broker_subnet_hash(const broker_subnet* s)
	{
	auto ss = reinterpret_cast<const broker::subnet*>(s);
	return std::hash<broker::subnet>{}(*ss);
	}
