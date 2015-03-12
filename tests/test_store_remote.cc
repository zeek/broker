#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/store/master.hh"
#include "broker/store/clone.hh"
#include "broker/store/frontend.hh"
#include "testsuite.h"
#include <iostream>
#include <map>
#include <vector>
#include <unistd.h>
#include <poll.h>

using namespace std;
using dataset = map<broker::data, broker::data>;

dataset get_contents(const broker::store::frontend& store)
	{
	dataset rval;

	for ( const auto& key : broker::store::keys(store) )
		{
		auto val = broker::store::lookup(store, key);
		if ( val ) rval.insert(make_pair(key, move(*val)));
		}

	return rval;
	}

bool compare_contents(const broker::store::frontend& store, const dataset& ds)
	{
	return get_contents(store) == ds;
	}

bool compare_contents(const broker::store::frontend& a,
                      const broker::store::frontend& b)
	{
	return get_contents(a) == get_contents(b);
	}

void wait_for(const broker::store::frontend& f, broker::data k,
              bool exists = true)
	{
	while ( broker::store::exists(f, k) != exists ) usleep(1000);
	}

int main(int argc, char** argv)
	{
	broker::init();
	broker::endpoint server("server");
	broker::store::master master(server, "mystore");

	broker::record myrecord({broker::record::field(broker::data("ddd")),
	                         broker::record::field(),
	                         broker::record::field(broker::data(333))});
	broker::table blue_pill{make_pair("1", "one"),
	                        make_pair("2", "two"),
	                        make_pair(3, "three"),
	                        make_pair("myrecord", myrecord)};
	dataset ds0;
	for ( const auto& p : blue_pill ) ds0.insert(p);
	ds0.insert(make_pair(blue_pill, "why?"));
	ds0.insert(make_pair("myrecord", myrecord));
	for ( const auto& p : ds0 ) master.insert(p.first, p.second);

	BROKER_TEST(compare_contents(master, ds0));

	// TODO: better way to distribute ports among tests so they can go parallel
	if ( ! server.listen(9999, "127.0.0.1") )
		{
		cerr << server.last_error() << endl;
		return 1;
		}

	broker::endpoint client("client");
	broker::store::frontend frontend(client, "mystore");
	broker::store::clone clone(client, "mystore",
	                          std::chrono::duration<double>(0.25));

	client.peer("127.0.0.1", 9999);

	if ( client.outgoing_connection_status().need_pop().front().status !=
	     broker::outgoing_connection_status::tag::established)
		{
		BROKER_TEST(false);
		return 1;
		}

	BROKER_TEST(compare_contents(frontend, ds0));
	BROKER_TEST(compare_contents(clone, ds0));

	master.insert("5", "five");
	BROKER_TEST(*broker::store::lookup(master, "5") == "five");
	BROKER_TEST(*broker::store::lookup(frontend, blue_pill) == "why?");
	auto myrec = *broker::store::lookup(frontend, "myrecord");
	BROKER_TEST(myrec == myrecord);
	BROKER_TEST(compare_contents(frontend, master));
	BROKER_TEST(compare_contents(clone, master));

	master.erase("5");
	BROKER_TEST(!broker::store::exists(master, "5"));
	BROKER_TEST(compare_contents(frontend, master));
	BROKER_TEST(compare_contents(clone, master));

	frontend.insert("5", "five");
	wait_for(master, "5");
	BROKER_TEST(compare_contents(frontend, master));
	BROKER_TEST(compare_contents(clone, master));

	frontend.erase("5");
	wait_for(master, "5", false);
	BROKER_TEST(compare_contents(frontend, master));
	BROKER_TEST(compare_contents(clone, master));

	clone.insert("5", "five");
	wait_for(master, "5");
	BROKER_TEST(compare_contents(frontend, master));
	BROKER_TEST(compare_contents(clone, master));

	clone.erase("5");
	wait_for(master, "5", false);
	BROKER_TEST(compare_contents(frontend, master));
	BROKER_TEST(compare_contents(clone, master));

	master.clear();
	BROKER_TEST(broker::store::size(master) == 0);
	BROKER_TEST(compare_contents(frontend, master));
	BROKER_TEST(compare_contents(clone, master));

	return BROKER_TEST_RESULT();
	}
