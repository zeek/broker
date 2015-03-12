#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/store/master.hh"
#include "broker/store/clone.hh"
#include "testsuite.h"
#include <map>
#include <unistd.h>

using namespace std;
using dataset = map<broker::data, broker::data>;

bool compare_contents(const broker::store::frontend& store, const dataset& ds)
	{
	dataset actual;

	for ( const auto& key : broker::store::keys(store) )
		{
		auto val = broker::store::lookup(store, key);
		if ( val ) actual.insert(make_pair(key, move(*val)));
		}

	return actual == ds;
	}

void wait_for(const broker::store::clone& c, broker::data k,
              bool exists = true)
	{
	while ( broker::store::exists(c, k) != exists ) usleep(1000);
	}

int main()
	{
	broker::init();
	broker::endpoint node("node0");
	broker::store::master m(node, "mystore");

	dataset ds0 = { make_pair("1", "one"),
	                make_pair("2", "two"),
	                make_pair("3", "three") };
	for ( const auto& p : ds0 ) m.insert(p.first, p.second);

	broker::store::clone c(node, "mystore");
	c.insert("4", "four");
	wait_for(c, "4");

	ds0.insert(make_pair("4", "four"));
	BROKER_TEST(compare_contents(c, ds0));
	BROKER_TEST(compare_contents(m, ds0));

	m.insert("5", "five");
	wait_for(c, "5");
	ds0.insert(make_pair("5", "five"));
	BROKER_TEST(compare_contents(c, ds0));
	BROKER_TEST(compare_contents(m, ds0));

	c.erase("5");
	wait_for(c, "5", false);
	ds0.erase("5");
	BROKER_TEST(compare_contents(c, ds0));
	BROKER_TEST(compare_contents(m, ds0));
	BROKER_TEST(broker::store::size(c) == ds0.size());
	BROKER_TEST(broker::store::size(m) == ds0.size());
	BROKER_TEST(*broker::store::lookup(c, "3") == "three");
	BROKER_TEST(*broker::store::lookup(m, "3") == "three");

	c.clear();
	wait_for(c, "1", false);
	BROKER_TEST(broker::store::size(c) == 0);
	BROKER_TEST(broker::store::size(m) == 0);

	return BROKER_TEST_RESULT();
	}
