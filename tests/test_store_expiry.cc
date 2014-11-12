#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/store/master.hh"
#include "broker/store/clone.hh"
#include "broker/store/sqlite_backend.hh"
#include "testsuite.hh"
#include <map>
#include <unistd.h>
#include <sys/time.h>

using namespace std;
using namespace broker;
using namespace broker::store;
using dataset = map<data, data>;

static double now()
	{
	struct timeval tv;
	gettimeofday(&tv, 0);
	return tv.tv_sec + (tv.tv_usec / 1000000.0);
	}

bool compare_contents(const frontend& store, const dataset& ds)
	{
	dataset actual;

	for ( const auto& key : keys(store) )
		{
		auto val = lookup(store, key);
		if ( val ) actual.insert(make_pair(key, move(*val)));
		}

	return actual == ds;
	}

void wait_for(const clone& c, data k, bool want_existence = true)
	{
	while ( exists(c, k) != want_existence ) usleep(1000);
	}

static bool open_db(const char* file, backend* b)
	{
	unlink(file);
	return ((sqlite_backend*)b)->open(file);
	}

int main(int argc, char** argv)
	{
	auto use_sqlite = argc > 1 && std::string(argv[1]) == "sqlite";

	broker::init();
	endpoint node("node0");
	expiration_time abs_expire = {now() + 5, expiration_time::tag::absolute};
	expiration_time mod_expire = {2};
	value pre_existing = {data("myval"), abs_expire};
	snapshot sss = {{{data("pre"), pre_existing}}, {}};
	unique_ptr<backend> mbacking;
	unique_ptr<backend> cbacking;

	if ( use_sqlite )
		{
		mbacking.reset(new sqlite_backend);
		cbacking.reset(new sqlite_backend);
		BROKER_TEST(open_db("expiry_test_master_db.tmp", mbacking.get()));
		BROKER_TEST(open_db("expiry_test_clone_db.tmp", cbacking.get()));
		}
	else
		{
		mbacking.reset(new memory_backend);
		cbacking.reset(new memory_backend);
		}

	mbacking->init(sss);
	master m(node, "mystore", move(mbacking));

	dataset ds0 = {
	                make_pair("pre",       "myval"),
	                make_pair("noexpire",  "one"),
	                make_pair("absexpire", "two"),
	                make_pair("refresh",   3),
	                make_pair("morerefresh", broker::set{2, 4, 6, 8}),
	                make_pair("norefresh", "four"),
	              };

	m.insert("noexpire",  "one");
	m.insert("absexpire", "two",   abs_expire);
	m.insert("refresh",   3, mod_expire);
	m.insert("morerefresh", broker::set{2, 4, 6, 8}, mod_expire);
	m.insert("norefresh", "four",  mod_expire);
	clone c(node, "mystore", chrono::duration<double>(0.25), move(cbacking));

	BROKER_TEST(compare_contents(c, ds0));
	BROKER_TEST(compare_contents(m, ds0));

	sleep(1);
	c.increment("refresh", 5);
	c.add_to_set("morerefresh", 0);
	sleep(1);
	m.decrement("refresh", 2);
	m.remove_from_set("morerefresh", 6);

	ds0.erase("norefresh");
	ds0["refresh"] = 6;
	ds0["morerefresh"] = broker::set{0, 2, 4, 8};

	wait_for(c, "norefresh", false);

	BROKER_TEST(compare_contents(c, ds0));
	BROKER_TEST(compare_contents(m, ds0));

	sleep(3);
	ds0.clear();
	ds0["noexpire"] = "one";

	BROKER_TEST(compare_contents(c, ds0));
	BROKER_TEST(compare_contents(m, ds0));

	return BROKER_TEST_RESULT();
	}
