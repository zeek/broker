#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/data/master.hh"
#include "broker/data/clone.hh"
#include "testsuite.hh"
#include <map>
#include <unistd.h>

using namespace std;
using dataset = map<broker::data::key, broker::data::value>;

bool compare_contents(const broker::data::frontend& store, const dataset& ds)
	{
	dataset actual;

	for ( const auto& key : broker::data::keys(store) )
		{
		auto val = broker::data::lookup(store, key);
		if ( val ) actual.insert(make_pair(key, *val.get()));
		}

	return actual == ds;
	}

void wait_for(const broker::data::clone& c, broker::data::key k,
              bool exists = true)
	{
	while ( broker::data::exists(c, k) != exists ) usleep(1000);
	}

int main()
	{
	broker::init();
	broker::endpoint node("node0");
	broker::data::master m(node, "mystore");

	dataset ds0 = { make_pair("1", "one"),
	                make_pair("2", "two"),
	                make_pair("3", "three") };
	for ( const auto& p : ds0 ) m.insert(p.first, p.second);

	broker::data::clone c(node, "mystore");
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
	BROKER_TEST(broker::data::size(c) == ds0.size());
	BROKER_TEST(broker::data::size(m) == ds0.size());
	BROKER_TEST(*broker::data::lookup(c, "3") == "three");
	BROKER_TEST(*broker::data::lookup(m, "3") == "three");

	c.clear();
	wait_for(c, "1", false);
	BROKER_TEST(broker::data::size(c) == 0);
	BROKER_TEST(broker::data::size(m) == 0);

	return BROKER_TEST_RESULT();
	}
