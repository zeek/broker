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
