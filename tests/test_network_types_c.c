#include "broker/broker.h"
#include "testsuite.h"
#include "string.h"

void test_port()
	{
	broker_port* p = broker_port_create();
	BROKER_TEST(broker_port_number(p) == 0);
	BROKER_TEST(broker_port_type(p) == broker_port_protocol_unknown);
	broker_port_delete(p);

	p = broker_port_from(22, broker_port_protocol_tcp);
	BROKER_TEST(broker_port_number(p) == 22);
	BROKER_TEST(broker_port_type(p) == broker_port_protocol_tcp);

	broker_port* q = broker_port_from(53, broker_port_protocol_udp);
	BROKER_TEST(broker_port_number(q) == 53);
	BROKER_TEST(broker_port_type(q) == broker_port_protocol_udp);
	BROKER_TEST(broker_port_eq(p, p));
	BROKER_TEST(! broker_port_eq(p, q));
	BROKER_TEST(broker_port_lt(p, q));
	broker_port_delete(p);
	broker_port_delete(q);
	}

void test_address_ipv4()
	{
	broker_address* x = broker_address_create();
	broker_address* y = broker_address_create();
	BROKER_TEST(broker_address_eq(x, y));
	BROKER_TEST(! broker_address_is_v4(x));
	BROKER_TEST(broker_address_is_v6(x));

	broker_address* a = 0;
	BROKER_TEST(broker_address_from_string(&a, "172.16.7.1"));
	broker_string* s = broker_address_to_string(a);
	BROKER_TEST(! strcmp(broker_string_data(s), "172.16.7.1"));
	BROKER_TEST(broker_address_is_v4(a));
	BROKER_TEST(! broker_address_is_v6(a));
	broker_string_delete(s);

	broker_address* localhost = 0;
	BROKER_TEST(broker_address_from_string(&localhost, "127.0.0.1"));
	BROKER_TEST(broker_address_lt(localhost, a));

	uint32_t n = 3232235691u;
	broker_address* b = broker_address_from_v4_host_bytes(&n);
	s = broker_address_to_string(b);
	BROKER_TEST(! strcmp(broker_string_data(s), "192.168.0.171"));
	broker_string_delete(s);
	broker_address_delete(x);
	broker_address_delete(y);
	broker_address_delete(a);
	broker_address_delete(b);
	broker_address_delete(localhost);
	}

void test_address_ipv6()
	{
	broker_address* unspec1 = broker_address_create();
	broker_address* unspec2 = 0;
	BROKER_TEST(broker_address_from_string(&unspec2, "::"));
	BROKER_TEST(broker_address_eq(unspec1, unspec2));

	broker_address* a = 0;
	broker_address* b = 0;
	broker_address* c = 0;
	broker_address* d = 0;
	broker_address* e = 0;
	broker_address* f = 0;

	BROKER_TEST(broker_address_from_string(&a,
	                            "2001:db8:0000:0000:0202:b3ff:fe1e:8329"));
	BROKER_TEST(broker_address_from_string(&b,
	                            "2001:db8:0:0:202:b3ff:fe1e:8329"));
	BROKER_TEST(broker_address_from_string(&c, "2001:db8::202:b3ff:fe1e:8329"));
	BROKER_TEST(broker_address_is_v6(a));
	BROKER_TEST(broker_address_is_v6(b));
	BROKER_TEST(broker_address_is_v6(c));
	BROKER_TEST(! broker_address_is_v4(a));
	BROKER_TEST(! broker_address_is_v4(b));
	BROKER_TEST(! broker_address_is_v4(c));
	BROKER_TEST(broker_address_eq(b, c));
	BROKER_TEST(broker_address_from_string(&d, "ff01::1"));

	uint8_t raw8[16] = { 0xdf, 0x00, 0x0d, 0xb8,
	                     0x00, 0x00, 0x00, 0x00,
	                     0x02, 0x02, 0xb3, 0xff,
	                     0xfe, 0x1e, 0x83, 0x28 };
	const uint32_t* p = (const uint32_t*)(raw8);
	e = broker_address_from_v6_network_bytes(p);
	uint32_t raw32[4] = { 0xdf000db8, 0x00000000, 0x0202b3ff, 0xfe1e8328 };
	p = (const uint32_t*)(raw32);
	f = broker_address_from_v6_host_bytes(p);
	BROKER_TEST(broker_address_eq(e, f));
	BROKER_TEST(! broker_address_mask(a, 129));
	BROKER_TEST(broker_address_mask(a, 128)); // No modification
	broker_address* am = 0;
	BROKER_TEST(broker_address_from_string(&am,
	                        "2001:db8:0000:0000:0202:b3ff:fe1e:8329"));
	BROKER_TEST(broker_address_eq(a, am));

	BROKER_TEST(broker_address_mask(a, 112));
	BROKER_TEST(broker_address_from_string(&am, "2001:db8::202:b3ff:fe1e:0"));
	BROKER_TEST(broker_address_eq(a, am));

	BROKER_TEST(broker_address_mask(a, 100));
	BROKER_TEST(broker_address_from_string(&am, "2001:db8::202:b3ff:f000:0"));
	BROKER_TEST(broker_address_eq(a, am));

	BROKER_TEST(broker_address_mask(a, 64));
	BROKER_TEST(broker_address_from_string(&am, "2001:db8::"));
	BROKER_TEST(broker_address_eq(a, am));

	BROKER_TEST(broker_address_mask(a, 3));
	BROKER_TEST(broker_address_from_string(&am, "2000::"));
	BROKER_TEST(broker_address_eq(a, am));

	BROKER_TEST(broker_address_mask(a, 0));
	BROKER_TEST(broker_address_from_string(&am, "::"));
	BROKER_TEST(broker_address_eq(a, am));
	broker_address_delete(a);
	broker_address_delete(b);
	broker_address_delete(c);
	broker_address_delete(d);
	broker_address_delete(e);
	broker_address_delete(f);
	broker_address_delete(am);
	broker_address_delete(unspec1);
	broker_address_delete(unspec2);
	}

void test_subnet()
	{
	broker_address* unspec = broker_address_create();
	broker_subnet* p = broker_subnet_create();
	BROKER_TEST(broker_address_eq(broker_subnet_network(p), unspec));
	broker_string* s = broker_subnet_to_string(p);
	BROKER_TEST(! strcmp(broker_string_data(s), "::/0"));
	broker_string_delete(s);

	broker_address* a = 0;
	broker_address* an = 0;
	BROKER_TEST(broker_address_from_string(&a, "192.168.0.1"));
	BROKER_TEST(broker_address_from_string(&an, "192.168.0.0"));
	broker_subnet* q = broker_subnet_from(a, 24);
	BROKER_TEST(broker_address_eq(an, broker_subnet_network(q)));
	BROKER_TEST(broker_subnet_length(q) == 24);
	s = broker_subnet_to_string(q);
	BROKER_TEST(! strcmp(broker_string_data(s), "192.168.0.0/24"));
	broker_string_delete(s);
	BROKER_TEST(broker_address_from_string(&a, "192.168.0.73"));
	BROKER_TEST(broker_subnet_contains(q, a));
	BROKER_TEST(broker_address_from_string(&a, "192.168.244.73"));
	BROKER_TEST(! broker_subnet_contains(q, a));

	BROKER_TEST(broker_address_from_string(&a,
	        "2001:db8:0000:0000:0202:b3ff:fe1e:8329"));
	BROKER_TEST(broker_address_from_string(&an, "2001:db8::"));
	broker_subnet* r = broker_subnet_from(a, 64);
	BROKER_TEST(broker_subnet_length(r) == 64);
	BROKER_TEST(broker_address_eq(an, broker_subnet_network(r)));
	s = broker_subnet_to_string(r);
	BROKER_TEST(! strcmp(broker_string_data(s), "2001:db8::/64"));
	broker_string_delete(s);

	BROKER_TEST(broker_address_from_string(&a, "2001:db8::cafe:babe"));
	BROKER_TEST(broker_subnet_contains(r, a));
	BROKER_TEST(broker_address_from_string(&a, "ff00::"));
	BROKER_TEST(! broker_subnet_contains(r, a));
	broker_address_delete(a);
	broker_address_delete(an);
	broker_address_delete(unspec);
	broker_subnet_delete(p);
	broker_subnet_delete(q);
	broker_subnet_delete(r);
	}

int main()
	{
	test_port();
	test_address_ipv4();
	test_address_ipv6();
	test_subnet();
	return BROKER_TEST_RESULT();
	}
