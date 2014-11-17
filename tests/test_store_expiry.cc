#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/store/master.hh"
#include "broker/store/clone.hh"
#include "broker/store/sqlite_backend.hh"

#ifdef HAVE_ROCKSDB
#include "broker/store/rocksdb_backend.hh"
#include <rocksdb/db.h>
#endif

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

static bool open_sqlite(string file, backend* b)
	{
	unlink(file.c_str());
	return ((sqlite_backend*)b)->open(file);
	}

#ifdef HAVE_ROCKSDB
static bool open_rocksdb(string file, backend* b)
	{
	rocksdb::DestroyDB(file, {});
	rocksdb::Options options;
	options.create_if_missing = true;
	return ((rocksdb_backend*)b)->open(file, options).ok();
	}
#endif

int main(int argc, char** argv)
	{
	std::string backend_name = argv[1];
	string db_name = "backend_test." + backend_name  + ".tmp";
	broker::init();
	endpoint node("node0");
	expiration_time abs_expire = {now() + 0.5, expiration_time::tag::absolute};
	expiration_time mod_expire = {0.2};
	value pre_existing = {data("myval"), abs_expire};
	snapshot sss = {{{data("pre"), pre_existing}}, {}};
	unique_ptr<backend> mbacking;
	unique_ptr<backend> cbacking;

#ifdef HAVE_ROCKSDB
	if ( backend_name == "rocksdb" )
		{
		mbacking.reset(new rocksdb_backend);
		cbacking.reset(new rocksdb_backend);
		BROKER_TEST(open_rocksdb(string("master.") + db_name, mbacking.get()));
		BROKER_TEST(open_rocksdb(string("clone.") + db_name, cbacking.get()));
		}
	else
#endif
	if ( backend_name == "sqlite" )
		{
		mbacking.reset(new sqlite_backend);
		cbacking.reset(new sqlite_backend);
		BROKER_TEST(open_sqlite(string("master.") + db_name, mbacking.get()));
		BROKER_TEST(open_sqlite(string("clone.") + db_name, cbacking.get()));
		}
	else if ( backend_name == "memory" )
		{
		mbacking.reset(new memory_backend);
		cbacking.reset(new memory_backend);
		}
	else
		return 1;

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

	usleep(100000);
	c.increment("refresh", 5);
	c.add_to_set("morerefresh", 0);
	usleep(100000);
	m.decrement("refresh", 2);
	m.remove_from_set("morerefresh", 6);

	ds0.erase("norefresh");
	ds0["refresh"] = 6;
	ds0["morerefresh"] = broker::set{0, 2, 4, 8};

	wait_for(c, "norefresh", false);

	BROKER_TEST(compare_contents(c, ds0));
	BROKER_TEST(compare_contents(m, ds0));

	usleep(300000);
	ds0.clear();
	ds0["noexpire"] = "one";

	BROKER_TEST(compare_contents(c, ds0));
	BROKER_TEST(compare_contents(m, ds0));

	return BROKER_TEST_RESULT();
	}
