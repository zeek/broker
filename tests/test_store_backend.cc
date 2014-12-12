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
	if ( backend_name == "rocksdb" || backend_name == "rocksdb_merge" )
		{
		rocksdb::DestroyDB(db_name, {});
		db = new rocksdb_backend;
		rocksdb::Options options;
		options.create_if_missing = true;

		if ( backend_name == "rocksdb_merge" )
			options.merge_operator.reset(new rocksdb_merge_operator);

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

	BROKER_TEST(! *db->pop_left("ingredients"));
	BROKER_TEST(! *db->pop_right("ingredients"));
	BROKER_TEST(db->push_left("ingredients", {"bacon"}) == 0);
	BROKER_TEST(db->push_right("ingredients", {"eggs"}) == 0);
	BROKER_TEST(db->push_left("ingredients", {"carrot", "potato"}) == 0);
	BROKER_TEST(db->push_left("ingredients", {"cabbage", "beet"}) == 0);
	BROKER_TEST(db->push_right("ingredients", {"beef", "pork"}) == 0);
	BROKER_TEST(db->push_right("ingredients", {"chicken", "turkey"}) == 0);

	BROKER_TEST(**db->pop_left("ingredients") == "cabbage");
	BROKER_TEST(**db->pop_right("ingredients") == "turkey");
	BROKER_TEST(**db->pop_right("ingredients") == "chicken");
	BROKER_TEST(**db->pop_left("ingredients") == "beet");
	BROKER_TEST(**db->pop_left("ingredients") == "carrot");
	BROKER_TEST(**db->pop_right("ingredients") == "pork");
	BROKER_TEST(**db->pop_right("ingredients") == "beef");
	BROKER_TEST(**db->pop_right("ingredients") == "eggs");
	BROKER_TEST(**db->pop_right("ingredients") == "bacon");
	BROKER_TEST(**db->pop_left("ingredients") == "potato");
	BROKER_TEST(! *db->pop_left("ingredients"));
	BROKER_TEST(! *db->pop_right("ingredients"));

	BROKER_TEST(db->erase("ingredients"));

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
	BROKER_TEST(db->increment("how many?", -1) == 0);
	BROKER_TEST(db->increment("how many?", -3) == 0);
	BROKER_TEST(db->increment("how many?", 6) == 0);
	BROKER_TEST(**db->lookup("how many?") == 961);
	BROKER_TEST(db->increment("c", 5) == 0);
	BROKER_TEST(db->increment("c", 9) == 0);
	BROKER_TEST(db->increment("c", 10) == 0);
	BROKER_TEST(db->increment("c", -3) == 0);
	BROKER_TEST(**db->lookup("c") == 21);
	BROKER_TEST(*db->size() == 5);

	BROKER_TEST(db->add_to_set("cook names", "Ken DeLozier") == 0);
	BROKER_TEST(db->add_to_set("cook names", "Tara Ochs") == 0);
	BROKER_TEST(db->add_to_set("cook names", "Tara Ochs") == 0);
	BROKER_TEST(db->add_to_set("cook names", "Katelyn Nacon") == 0);
	BROKER_TEST(db->remove_from_set("cook names", "Katelyn Nacon") == 0);
	BROKER_TEST(db->add_to_set("cook names", "Justin Scott") == 0);
	BROKER_TEST(db->add_to_set("cook names", "Morgan Burch") == 0);
	BROKER_TEST(db->add_to_set("cook names", "Linda Miller") == 0);
	BROKER_TEST(db->remove_from_set("cook names", "Layla Neal") == 0);
	BROKER_TEST(db->add_to_set("cook names", "Layla Neal") == 0);
	BROKER_TEST(db->remove_from_set("cook names", "Lila Neal") == 0);
	BROKER_TEST(db->remove_from_set("cook names", "Lila Neal") == 0);

	broker::set cook_names{
	"Ken DeLozier",
	"Tara Ochs",
	"Justin Scott",
	"Morgan Burch",
	"Linda Miller",
	"Layla Neal",
	};

	BROKER_TEST(**db->lookup("cook names") == cook_names);
	BROKER_TEST(*db->size() == 6);

	broker::set more_cook_names{
	"Kayte Giralt",
	"Truman Orr",
	"Jayla James",
	"Cameron Markeles",
	};

	BROKER_TEST(db->insert("more cook names", more_cook_names));
	BROKER_TEST(db->add_to_set("more cook names", "Kayte Giralt") == 0);
	BROKER_TEST(db->remove_from_set("more cook names", "Truman Orr") == 0);
	more_cook_names.erase("Truman Orr");
	BROKER_TEST(db->add_to_set("more cook names", "Zack Shires") == 0);
	more_cook_names.emplace("Zack Shires");
	BROKER_TEST(db->remove_from_set("more cook names",
	                                "Gwydion Lashlee-Walton") == 0);

	BROKER_TEST(**db->lookup("more cook names") == more_cook_names);
	BROKER_TEST(*db->size() == 7);

	delete db;

#ifdef HAVE_ROCKSDB
	if ( backend_name == "rocksdb" || backend_name == "rocksdb_merge" )
		{
		db = new rocksdb_backend;
		rocksdb::Options options;

		if ( backend_name == "rocksdb_merge" )
			options.merge_operator.reset(new rocksdb_merge_operator);

		BROKER_TEST(((rocksdb_backend*)db)->open(db_name, options).ok());
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

	BROKER_TEST(*db->size() == 7);
	BROKER_TEST(*db->exists("great"));
	BROKER_TEST(db->erase("great"));
	BROKER_TEST(!*db->exists("great"));
	BROKER_TEST(*db->size() == 6);

	dataset ds0 = {
	    make_pair("too many cooks", "too many cooks"),
	    make_pair("many cooks", "never too many"),
	    make_pair("how many?", 961),
	    make_pair("c", 21),
	    make_pair("cook names", cook_names),
	    make_pair("more cook names", more_cook_names),
	};

	BROKER_TEST(compare_contents(db, ds0));
	BROKER_TEST(db->clear());
	BROKER_TEST(*db->size() == 0);

	return BROKER_TEST_RESULT();
	}
