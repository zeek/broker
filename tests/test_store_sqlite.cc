#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/store/master.hh"
#include "broker/store/sqlite_backend.hh"
#include "testsuite.hh"
#include <unistd.h>

using namespace std;
using namespace broker;
using namespace broker::store;

using dataset = map<data, data>;

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

int main()
	{
	broker::init();
	endpoint node("node0");
	sqlite_backend* sb0 = new sqlite_backend;
	unlink("data0.tmp");
	BROKER_TEST(sb0->open("data0.tmp", {}));
	{
	master data0(node, "data0", unique_ptr<backend>(sb0));
	BROKER_TEST(size(data0) == 0);
	data0.insert("too many cooks", "too many cooks");
	BROKER_TEST(size(data0) == 1);
	BROKER_TEST(exists(data0, "too many cooks"));
	BROKER_TEST(!exists(data0, "not too many cooks"));
	BROKER_TEST(*lookup(data0, "too many cooks") == "too many cooks");
	data0.insert("many cooks", "too many");
	BROKER_TEST(size(data0) == 2);
	BROKER_TEST(*lookup(data0, "too many cooks") == "too many cooks");
	BROKER_TEST(*lookup(data0, "many cooks") == "too many");
	BROKER_TEST(exists(data0, "too many cooks"));
	BROKER_TEST(exists(data0, "many cooks"));
	BROKER_TEST(!exists(data0, "not too many cooks"));
	data0.insert("great", "it works");
	BROKER_TEST(size(data0) == 3);
	data0.insert("many cooks", "never too many");
	BROKER_TEST(*lookup(data0, "many cooks") == "never too many");
	BROKER_TEST(size(data0) == 3);
	data0.insert("how many?", 954);
	BROKER_TEST(size(data0) == 4);
	data0.increment("how many?", 5);
	BROKER_TEST(*lookup(data0, "how many?") == 959);
	}

	sb0 = new sqlite_backend;
	BROKER_TEST(sb0->open("data0.tmp"));
	master data0(node, "data0", unique_ptr<backend>(sb0));
	BROKER_TEST(size(data0) == 4);
	BROKER_TEST(exists(data0, "great"));
	data0.erase("great");
	BROKER_TEST(!exists(data0, "great"));
	BROKER_TEST(size(data0) == 3);

	dataset ds0 = {
	    make_pair("too many cooks", "too many cooks"),
	    make_pair("many cooks", "never too many"),
	    make_pair("how many?", 959),
	};
	BROKER_TEST(compare_contents(data0, ds0));
	data0.clear();
	BROKER_TEST(size(data0) == 0);

	broker::done();
	return BROKER_TEST_RESULT();
	}
