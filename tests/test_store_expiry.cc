#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/store/master.hh"
#include "broker/store/clone.hh"
#include "testsuite.hh"
#include <map>
#include <unistd.h>
#include <sys/time.h>

using namespace std;
using dataset = map<broker::data, broker::data>;

static double now()
	{
	struct timeval tv;
	gettimeofday(&tv, 0);
	return tv.tv_sec + (tv.tv_usec / 1000000.0);
	}

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
	using namespace broker;

	broker::init();
	endpoint node("node0");
	store::expiration_time abs_expire = {now() + 5,
	                                     store::expiration_time::tag::absolute};
	store::expiration_time mod_expire = {2};
	store::value pre_existing = {data("myval"), abs_expire};
	store::snapshot sss = {{{data("pre"), pre_existing}}, {}};
	unique_ptr<store::store> backing(new store::mem_store{sss});
	broker::store::master m(node, "mystore", move(backing));

	dataset ds0 = {
	                make_pair("pre",       "myval"),
	                make_pair("noexpire",  "one"),
	                make_pair("absexpire", "two"),
	                make_pair("refresh",   "three"),
	                make_pair("norefresh", "four"),
	              };

	m.insert("noexpire",  "one");
	m.insert("absexpire", "two",   abs_expire);
	m.insert("refresh",   "three", mod_expire);
	m.insert("norefresh", "four",  mod_expire);
	broker::store::clone c(node, "mystore");

	BROKER_TEST(compare_contents(c, ds0));
	BROKER_TEST(compare_contents(m, ds0));

	sleep(1);
	// TODO: change this to an in-place modification once those are implemented.
	m.insert("refresh",   "three", mod_expire);
	sleep(1);
	// TODO: change this to an in-place modification once those are implemented.
	m.insert("refresh",   "three", mod_expire);
	ds0.erase("norefresh");

	BROKER_TEST(compare_contents(c, ds0));
	BROKER_TEST(compare_contents(m, ds0));

	sleep(3);
	ds0.clear();
	ds0["noexpire"] = "one";

	BROKER_TEST(compare_contents(c, ds0));
	BROKER_TEST(compare_contents(m, ds0));

	return BROKER_TEST_RESULT();
	}
