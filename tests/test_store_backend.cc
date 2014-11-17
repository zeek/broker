#include "broker/broker.hh"
#include "testsuite.hh"
#include <unistd.h>
#include "broker/store/memory_backend.hh"
#include "broker/store/sqlite_backend.hh"

#ifdef HAVE_ROCKSDB
#include "broker/store/rocksdb_backend.hh"
#include <rocksdb/db.h>
#endif

using namespace std;
using namespace broker;
using namespace broker::store;

using dataset = map<data, data>;

bool compare_contents(backend* db, const dataset& ds)
	{
	dataset actual;

	auto res = db->keys();
	BROKER_TEST(res);

	for ( const auto& key : *res )
		{
		auto val = *db->lookup(key);
		if ( val ) actual.insert(make_pair(key, move(*val)));
		}

	return actual == ds;
	}

int main(int argc, char** argv)
	{
	std::string backend_name = argv[1];
	string db_name = "backend_test." + backend_name  + ".tmp";
	broker::init();
	backend* db;

#ifdef HAVE_ROCKSDB
	if ( backend_name == "rocksdb" )
		{
		rocksdb::DestroyDB(db_name, {});
		db = new rocksdb_backend;
		rocksdb::Options options;
		options.create_if_missing = true;
		BROKER_TEST(((rocksdb_backend*)db)->open(db_name, options).ok());
		}
	else
#endif
	if ( backend_name == "sqlite" )
		{
		unlink(db_name.c_str());
		db = new sqlite_backend;
		BROKER_TEST(((sqlite_backend*)db)->open(db_name, {}));
		}
	else if ( backend_name == "memory" )
		db = new memory_backend;
	else
		return 1;

	BROKER_TEST(*db->size() == 0);
	BROKER_TEST(db->insert("too many cooks", "too many cooks"));
	BROKER_TEST(*db->size() == 1);
	BROKER_TEST(*db->exists("too many cooks"));
	BROKER_TEST(!*db->exists("not too many cooks"));
	BROKER_TEST(**db->lookup("too many cooks") == "too many cooks");
	BROKER_TEST(db->insert("many cooks", "too many"));
	BROKER_TEST(*db->size() == 2);
	BROKER_TEST(**db->lookup("too many cooks") == "too many cooks");
	BROKER_TEST(**db->lookup("many cooks") == "too many");
	BROKER_TEST(*db->exists("too many cooks"));
	BROKER_TEST(*db->exists("many cooks"));
	BROKER_TEST(!*db->exists("not too many cooks"));
	BROKER_TEST(db->insert("great", "it works"));
	BROKER_TEST(*db->size() == 3);
	BROKER_TEST(db->insert("many cooks", "never too many"));
	BROKER_TEST(**db->lookup("many cooks") == "never too many");
	BROKER_TEST(*db->size() == 3);
	BROKER_TEST(db->insert("how many?", 954));
	BROKER_TEST(*db->size() == 4);
	BROKER_TEST(db->increment("how many?", 5) == 0);
	BROKER_TEST(**db->lookup("how many?") == 959);
	delete db;

#ifdef HAVE_ROCKSDB
	if ( backend_name == "rocksdb" )
		{
		db = new rocksdb_backend;
		BROKER_TEST(((rocksdb_backend*)db)->open(db_name).ok());
		}
	else
#endif
	if ( backend_name == "sqlite" )
		{
		db = new sqlite_backend;
		BROKER_TEST(((sqlite_backend*)db)->open(db_name));
		}
	else if ( backend_name == "memory" )
		return BROKER_TEST_RESULT();

	BROKER_TEST(*db->size() == 4);
	BROKER_TEST(*db->exists("great"));
	BROKER_TEST(db->erase("great"));
	BROKER_TEST(!*db->exists("great"));
	BROKER_TEST(*db->size() == 3);

	dataset ds0 = {
	    make_pair("too many cooks", "too many cooks"),
	    make_pair("many cooks", "never too many"),
	    make_pair("how many?", 959),
	};

	BROKER_TEST(compare_contents(db, ds0));
	BROKER_TEST(db->clear());
	BROKER_TEST(*db->size() == 0);

	return BROKER_TEST_RESULT();
	}
