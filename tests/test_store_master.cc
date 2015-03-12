#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/store/master.hh"
#include "testsuite.h"
#include <map>

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

void populate(const broker::store::master& m, const dataset& ds)
	{ for ( const auto& p : ds ) m.insert(p.first, p.second); }

int main()
	{
	broker::init();
	broker::endpoint node("node0");
	broker::store::master data0(node, "data0");
	broker::store::master data1(node, "data1");

	dataset ds0 = { make_pair("1", "one"),
	                make_pair("2", "two"),
	                make_pair("3", "three") };
	dataset ds1 = { make_pair("a", "alpha"),
	                make_pair("b", "bravo"),
	                make_pair("c", "charlie") };

	populate(data0, ds0);
	populate(data1, ds1);

	BROKER_TEST(compare_contents(data0, ds0));
	BROKER_TEST(compare_contents(data1, ds1));
	BROKER_TEST(broker::store::exists(data0, "1"));
	BROKER_TEST(!broker::store::exists(data0, "a"));
	BROKER_TEST(*broker::store::lookup(data1, "b") == "bravo");
	BROKER_TEST(!broker::store::lookup(data0, "nope"));
	ds0.erase("2");
	data0.erase("2");
	BROKER_TEST(compare_contents(data0, ds0));
	BROKER_TEST(!broker::store::exists(data0, "2"));
	BROKER_TEST(broker::store::size(data0) == 2);
	data1.clear();
	ds1.clear();
	BROKER_TEST(compare_contents(data1, ds1));
	BROKER_TEST(broker::store::size(data1) == 0);

	return BROKER_TEST_RESULT();
	}
