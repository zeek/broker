#include "broker/address.hh"
#include "broker/subnet.hh"
#include "broker/port.hh"
#include "testsuite.h"

using namespace std;
using namespace broker;

void test_port()
	{
	port p;
	BROKER_TEST(p.number() == 0);
	BROKER_TEST(p.type() == port::protocol::unknown);

	p = port(22, port::protocol::tcp);
	BROKER_TEST(p.number() == 22);
	BROKER_TEST(p.type() == port::protocol::tcp);

	port q(53, port::protocol::udp);
	BROKER_TEST(q.number() == 53);
	BROKER_TEST(q.type() == port::protocol::udp);

	BROKER_TEST(p != q);
	BROKER_TEST(p < q);
	}

void test_address_ipv4()
	{
	// IPv4
	address x;
	address y;
	BROKER_TEST(x == y);
	BROKER_TEST(! x.is_v4());
	BROKER_TEST(x.is_v6());

	auto a = *address::from_string("172.16.7.1");
	BROKER_TEST(to_string(a) == "172.16.7.1");
	BROKER_TEST(a.is_v4());
	BROKER_TEST(! a.is_v6());

	auto localhost = *address::from_string("127.0.0.1");
	BROKER_TEST(to_string(localhost) == "127.0.0.1");
	BROKER_TEST(localhost.is_v4());

	BROKER_TEST(localhost < a);

	uint32_t n = 3232235691;
	address b{&n, address::family::ipv4, address::byte_order::host};
	BROKER_TEST(to_string(b) == "192.168.0.171");
	}

void test_address_ipv6()
	{
	// IPv6
	BROKER_TEST(address() == *address::from_string("::"));

	auto a = *address::from_string("2001:db8:0000:0000:0202:b3ff:fe1e:8329");
	auto b = *address::from_string("2001:db8:0:0:202:b3ff:fe1e:8329");
	auto c = *address::from_string("2001:db8::202:b3ff:fe1e:8329");
	BROKER_TEST(a.is_v6() && b.is_v6() && c.is_v6());
	BROKER_TEST(! (a.is_v4() || b.is_v4() || c.is_v4()));
	BROKER_TEST(a == b && b == c);

	auto d = *address::from_string("ff01::1");

	uint8_t raw8[16] = { 0xdf, 0x00, 0x0d, 0xb8,
	                     0x00, 0x00, 0x00, 0x00,
	                     0x02, 0x02, 0xb3, 0xff,
	                     0xfe, 0x1e, 0x83, 0x28 };
	auto p = reinterpret_cast<const uint32_t*>(raw8);
	address e(p, address::family::ipv6, address::byte_order::network);

	uint32_t raw32[4] = { 0xdf000db8, 0x00000000, 0x0202b3ff, 0xfe1e8328 };
	p = reinterpret_cast<const uint32_t*>(raw32);
	address f(p, address::family::ipv6, address::byte_order::host);
	BROKER_TEST(f == e);

	BROKER_TEST(! a.mask(129));
	BROKER_TEST(a.mask(128)); // No modification
	BROKER_TEST(a == *address::from_string("2001:db8:0000:0000:0202:b3ff:fe1e:8329"));
	BROKER_TEST(a.mask(112));
	BROKER_TEST(a == *address::from_string("2001:db8::202:b3ff:fe1e:0"));
	BROKER_TEST(a.mask(100));
	BROKER_TEST(a == *address::from_string("2001:db8::202:b3ff:f000:0"));
	BROKER_TEST(a.mask(64));
	BROKER_TEST(a == *address::from_string("2001:db8::"));
	BROKER_TEST(a.mask(3));
	BROKER_TEST(a == *address::from_string("2000::"));
	BROKER_TEST(a.mask(0));
	BROKER_TEST(a == *address::from_string("::"));
	}

void test_subnet()
	{
	subnet p;
	BROKER_TEST(p.network() == *address::from_string("::"));
	BROKER_TEST(p.length() == 0);
	BROKER_TEST(to_string(p) == "::/0");

	auto a = *address::from_string("192.168.0.1");
	subnet q{a, 24};
	BROKER_TEST(q.network() == *address::from_string("192.168.0.0"));
	BROKER_TEST(q.length() == 24);
	BROKER_TEST(to_string(q) == "192.168.0.0/24");
	BROKER_TEST(q.contains(*address::from_string("192.168.0.73")));
	BROKER_TEST(! q.contains(*address::from_string("192.168.244.73")));

	auto b = *address::from_string("2001:db8:0000:0000:0202:b3ff:fe1e:8329");
	subnet r{b, 64};
	BROKER_TEST(r.length() == 64);
	BROKER_TEST(r.network() == *address::from_string("2001:db8::"));
	BROKER_TEST(to_string(r) == "2001:db8::/64");
	BROKER_TEST(r.contains(*address::from_string("2001:db8::cafe:babe")));
	BROKER_TEST(! r.contains(*address::from_string("ff00::")));
	}

int main()
	{
	test_port();
	test_address_ipv4();
	test_address_ipv6();
	test_subnet();
	return BROKER_TEST_RESULT();
	}
