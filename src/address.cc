#include "broker/address.hh"
#include "broker/util/hash.hh"
#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

const std::array<uint8_t, 12> broker::address::v4_mapped_prefix =
    {{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff }};

broker::util::optional<broker::address>
broker::address::from_string(const std::string& s)
	{
	address rval;

	if ( s.find(':') == std::string::npos ) // IPv4.
		{
		std::copy(v4_mapped_prefix.begin(), v4_mapped_prefix.end(),
		          rval.addr.begin());

		// Parse the address directly instead of using inet_pton since
		// some platforms have more sensitive implementations than others
		// that can't e.g. handle leading zeroes.
		int a[4];
		int n = sscanf(s.data(), "%d.%d.%d.%d", a + 0, a + 1, a + 2, a + 3);

		if ( n != 4 || a[0] < 0 || a[1] < 0 || a[2] < 0 || a[3] < 0 ||
		     a[0] > 255 || a[1] > 255 || a[2] > 255 || a[3] > 255 )
			return {};

		uint32_t addr = (a[0] << 24) | (a[1] << 16) | (a[2] << 8) | a[3];
		auto p = reinterpret_cast<uint32_t*>(&rval.addr[12]);
		*p = htonl(addr);
		return std::move(rval);
		}

	if ( inet_pton(AF_INET6, s.c_str(), &rval.addr) <= 0 )
		return {};

	return std::move(rval);
	}

broker::address::address()
	{ addr.fill(0); }

broker::address::address(const uint32_t* bytes, family fam, byte_order order)
	{
	if ( fam == family::ipv4 )
		{
		std::copy(v4_mapped_prefix.begin(), v4_mapped_prefix.end(),
		          addr.begin());

		auto p = reinterpret_cast<uint32_t*>(&addr[12]);
		*p = (order == byte_order::host) ? htonl(*bytes) : *bytes;
		}
	else
		{
		std::copy(bytes, bytes + 4, reinterpret_cast<uint32_t*>(&addr));

		if ( order == byte_order::host )
			for ( auto i = 0; i < 4; ++i )
				{
				auto p = reinterpret_cast<uint32_t*>(&addr[i * 4]);
				*p = htonl(*p);
				}
		}
	}

static uint32_t bit_mask32(int bottom_bits)
	{
	if ( bottom_bits >= 32 )
		return 0xffffffff;
	return (((uint32_t) 1) << bottom_bits) - 1;
	}

bool broker::address::mask(uint8_t top_bits_to_keep)
	{
	if ( top_bits_to_keep > 128 )
		return false;

	uint32_t mask[4] = { 0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff };
	std::ldiv_t res = std::ldiv(top_bits_to_keep, 32);

	if ( res.quot < 4 )
		mask[res.quot] = htonl(mask[res.quot] & ~bit_mask32(32 - res.rem));

	for ( auto i = res.quot + 1; i < 4; ++i )
		mask[i] = 0;

	auto p = reinterpret_cast<uint32_t*>(&addr);

	for ( auto i = 0; i < 4; ++i )
		p[i] &= mask[i];

	return true;
	}

bool broker::address::is_v4() const
	{ return memcmp(&addr, &v4_mapped_prefix, 12) == 0; }

bool broker::address::is_v6() const
	{ return ! is_v4(); }

const std::array<uint8_t, 16>& broker::address::bytes() const
	{ return addr; }

std::string broker::to_string(const broker::address& a)
	{
	char buf[INET6_ADDRSTRLEN];

	if ( a.is_v4() )
		{
		if ( ! inet_ntop(AF_INET, &a.addr[12], buf, INET_ADDRSTRLEN) )
			return {};
		}
	else
		{
		if ( ! inet_ntop(AF_INET6, &a.addr, buf, INET6_ADDRSTRLEN) )
			return {};
		}

	return buf;
	}

std::ostream& broker::operator<<(std::ostream& out, const broker::address& a)
	{ return out << to_string(a); }

bool broker::operator==(const broker::address& lhs, const broker::address& rhs)
	{ return lhs.addr == rhs.addr; }

bool broker::operator<(const broker::address& lhs, const broker::address& rhs)
	{ return lhs.addr < rhs.addr; }

size_t std::hash<broker::address>::operator()(const broker::address& v) const
	{ return broker::util::hash_range(v.bytes().begin(), v.bytes().end()); }

// Begin C API
#include "broker/broker.h"
using std::nothrow;

broker_address* broker_address_create()
	{
	return reinterpret_cast<broker_address*>(new (nothrow) broker::address());
	}

void broker_address_delete(broker_address* a)
	{ delete reinterpret_cast<broker::address*>(a); }

broker_address* broker_address_copy(const broker_address* a)
	{
	auto aa = reinterpret_cast<const broker::address*>(a);
	return reinterpret_cast<broker_address*>(
	            new (nothrow) broker::address(*aa));
	}

int broker_address_from_string(broker_address** dst, const char* s)
	{
	try
		{
		auto opt_address = broker::address::from_string(s);

		if ( ! opt_address )
			return 0;

		if ( *dst )
			{
			*reinterpret_cast<broker::address*>(*dst) = std::move(*opt_address);
			return 1;
			}

		auto rval = new broker::address(std::move(*opt_address));
		*dst = reinterpret_cast<broker_address*>(rval);
		return 1;
		}
	catch ( std::bad_alloc& )
		{ return 0; }
	}

broker_address* broker_address_from_v4_host_bytes(const uint32_t* bytes)
	{
	return reinterpret_cast<broker_address*>(
	       new (nothrow) broker::address(bytes,
	                                     broker::address::family::ipv4,
	                                     broker::address::byte_order::host));
	}

broker_address* broker_address_from_v4_network_bytes(const uint32_t* bytes)
	{
	return reinterpret_cast<broker_address*>(
	       new (nothrow) broker::address(bytes,
	                                     broker::address::family::ipv4,
	                                     broker::address::byte_order::network));

	}

broker_address* broker_address_from_v6_host_bytes(const uint32_t* bytes)
	{
	return reinterpret_cast<broker_address*>(
	       new (nothrow) broker::address(bytes,
	                                     broker::address::family::ipv6,
	                                     broker::address::byte_order::host));

	}

broker_address* broker_address_from_v6_network_bytes(const uint32_t* bytes)
	{
	return reinterpret_cast<broker_address*>(
	       new (nothrow) broker::address(bytes,
	                                     broker::address::family::ipv6,
	                                     broker::address::byte_order::network));
	}

void broker_address_set(broker_address* dst, broker_address* src)
	{
	auto d = reinterpret_cast<broker::address*>(dst);
	auto s = reinterpret_cast<broker::address*>(src);
	*d = *s;
	}

int broker_address_mask(broker_address* a, uint8_t top_bits_to_keep)
	{ return reinterpret_cast<broker::address*>(a)->mask(top_bits_to_keep); }

int broker_address_is_v4(const broker_address* a)
	{ return reinterpret_cast<const broker::address*>(a)->is_v4(); }

int broker_address_is_v6(const broker_address* a)
	{ return reinterpret_cast<const broker::address*>(a)->is_v6(); }

const uint8_t* broker_address_bytes(const broker_address* a)
	{ return &reinterpret_cast<const broker::address*>(a)->bytes()[0]; }

broker_string* broker_address_to_string(const broker_address* a)
	{
	auto aa = reinterpret_cast<const broker::address*>(a);

	try
		{
		auto rval = broker::to_string(*aa);
		return reinterpret_cast<broker_string*>(
		    new std::string(std::move(rval)));
		}
	catch ( std::bad_alloc& )
		{ return nullptr; }
	}

int broker_address_eq(const broker_address* a, const broker_address* b)
	{
	auto aa = reinterpret_cast<const broker::address*>(a);
	auto bb = reinterpret_cast<const broker::address*>(b);
	return *aa == *bb;
	}

int broker_address_lt(const broker_address* a, const broker_address* b)
	{
	auto aa = reinterpret_cast<const broker::address*>(a);
	auto bb = reinterpret_cast<const broker::address*>(b);
	return *aa < *bb;
	}

size_t broker_address_hash(const broker_address* a)
	{
	auto aa = reinterpret_cast<const broker::address*>(a);
	return std::hash<broker::address>{}(*aa);
	}
