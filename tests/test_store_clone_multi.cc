#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/store/master.hh"
#include "broker/store/clone.hh"
#include <broker/store/backend.hh>
#include <broker/store/memory_backend.hh>
#include "broker/report.hh"
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
	// init debugging/reporting 
	//broker::report::init();

	broker::endpoint node0("node0");
	broker::store::master m(node0, "mystore", 
													std::unique_ptr<broker::store::backend>(new broker::store::memory_backend), 
													broker::store::GLOBAL_STORE);

	dataset ds0 = { make_pair("1", "one"),
	                make_pair("2", "two"),
	                make_pair("3", "three") };
	for ( const auto& p : ds0 ) m.insert(p.first, p.second);

	// more peers
	broker::endpoint node1("node1");
	broker::endpoint node2("node2");

	node1.peer(node0);
	if ( node1.outgoing_connection_status().need_pop().front().status !=
	     broker::outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	node2.peer(node1);
	if ( node2.outgoing_connection_status().need_pop().front().status !=
	     broker::outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	broker::store::clone c(node2, "mystore");
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
