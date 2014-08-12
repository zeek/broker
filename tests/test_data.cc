#include "broker/broker.hh"
#include "broker/endpoint.hh"
#include "broker/data/frontend.hh"
#include "broker/data/clone.hh"
#include "broker/data/master.hh"
#include "broker/data/types.hh"

#include <cstdio>
#include <iostream>
#include <sstream>
#include <string>
#include <functional>
#include <unordered_set>
#include <unistd.h>
#include <mutex>
#include <cinttypes>

using namespace std;

int cb_result_to_int(broker::data::query_result res)
	{
	return static_cast<int>(res);
	}

void lookup_cb(broker::data::key key, unique_ptr<broker::data::value> val,
               void* cookie, broker::data::query_result res)
	{
	lock_guard<mutex> lock(*static_cast<mutex*>(cookie));

	if ( res != broker::data::query_result::success )
		{
		printf("lookup_cb failed: %d\n", cb_result_to_int(res));
		return;
		}

	printf("lookup_cb: %s -> %s\n", key.c_str(), val ? val.get()->c_str()
	                                                 : "null");
	}

void haskey_cb(broker::data::key key, bool exists, void* cookie,
               broker::data::query_result res)
	{
	lock_guard<mutex> lock(*static_cast<mutex*>(cookie));

	if ( res != broker::data::query_result::success )
		{
		printf("haskey_cb failed: %d\n", cb_result_to_int(res));
		return;
		}

	printf("haskey_cb: %s -- %s\n", key.c_str(), exists ? "yes" : "no");
	}

void keys_cb(std::unordered_set<broker::data::key> keys, void* cookie,
             broker::data::query_result res)
	{
	lock_guard<mutex> lock(*static_cast<mutex*>(cookie));

	if ( res != broker::data::query_result::success )
		{
		printf("keys_cb failed: %d\n", cb_result_to_int(res));
		return;
		}

	printf("keys_cb:\n");
	for ( const auto& k : keys )
		printf("\t%s\n", k.c_str());
	}

void size_cb(uint64_t size, void* cookie, broker::data::query_result res)
	{
	lock_guard<mutex> lock(*static_cast<mutex*>(cookie));

	if ( res != broker::data::query_result::success )
		{
		printf("size_cb failed: %d\n", cb_result_to_int(res));
		return;
		}

	printf("size_cb: %" PRIu64 "\n", size);
	}

static void dump_store_contents(const broker::data::frontend& store)
	{
	string header = "==== " + store.topic() + " Contents ====";
	std::stringstream ss;
	ss << header << std::endl;

	auto keys = store.keys();

	for ( const auto& key : keys )
		{
		auto val = store.lookup(key);
		if ( val ) ss << key << " -> " << *val.get() << endl;
		}

	for ( size_t i = 0; i < header.size(); ++i )
		ss << "=";

	ss << endl;
	cout << ss.str();
	}

int main(int argc, char** argv)
	{
	broker::init();
	mutex mtx;
	auto timeout = chrono::seconds(10);

	if ( argc == 1 )
		{
		broker::endpoint node{"node0"};

		broker::data::master d0{node, "my_data"};

		if ( ! node.listen(9999, "127.0.0.1") )
			{
			cerr << node.last_error() << endl;
			return 1;
			}

		for ( auto i = 0; ; ++i )
			{
			stringstream ss;
			ss << i;
			d0.insert(node.name(), ss.str());

			sleep(1);
			dump_store_contents(d0);
			sleep(1);
			}
		}
	else
		{
		broker::endpoint node{argv[1]};

		node.peer("localhost", 9999).handshake();

		broker::data::clone d0{node, "my_data"};
		broker::data::frontend d1{node, "my_data"};

		if ( false )
			{
			// Just demonstrating the interface...
			d0.insert("key0", "blah"); // will be applied to remote master.
			d1.insert("key0", "abc");  // also applied to remote master.

			auto val = d0.lookup("key0"); // get from local clone.
			d1.lookup("key0", timeout, lookup_cb, &mtx);// queries remote store

			auto exists = d0.has_key("key0"); // get from local clone.
			d1.has_key("key0", timeout, haskey_cb, &mtx);// queries remote store

			d0.erase("key0"); // applied to remote master.
			d1.erase("key0"); // applied to remote master.

			d0.clear(); // applied to remote master.
			d1.clear(); // applied to remote master.

			auto keys = d0.keys(); // get from local clone.
			d1.keys(timeout, keys_cb, &mtx); // queries remote store

			auto sz = d0.size(); // determined from local clone
			d1.size(timeout, size_cb, &mtx); // queries remote store
			}

		for ( auto i = 0; ; ++i )
			{
			stringstream ss;
			ss << i;
			d0.insert(node.name(), ss.str());

			auto sz = d0.size();
			printf("size: %" PRIu64 "\n", sz);
			sz = d1.size();
			printf("size: %" PRIu64 "\n", sz);
			d0.size(timeout, size_cb, &mtx);
			d1.size(timeout, size_cb, &mtx);

			auto exists = d0.has_key(node.name());
			printf("key '%s' exists: %s\n", node.name().c_str(),
			       exists ? "yes" : "no");
			exists = d1.has_key(node.name());
			printf("key '%s' exists: %s\n", node.name().c_str(),
			       exists ? "yes" : "no");
			exists = d1.has_key("nope");
			printf("key '%s' exists: %s\n", "nope", exists ? "yes" : "no");
			d0.has_key("nope", timeout, haskey_cb, &mtx);
			d1.has_key(node.name(), timeout, haskey_cb, &mtx);

			auto val = d0.lookup(node.name());
			printf("lookup: %s -> %s\n", node.name().c_str(),
			       val ? val.get()->c_str() : "null");
			val = d1.lookup(node.name());
			printf("lookup: %s -> %s\n", node.name().c_str(),
			       val ? val.get()->c_str() : "null");
			d0.lookup("nope", timeout, lookup_cb, &mtx);
			d1.lookup(node.name(), timeout, lookup_cb, &mtx);

			d1.keys(timeout, keys_cb, &mtx);
			d1.keys(chrono::seconds(0), keys_cb, &mtx);

			sleep(1);
			dump_store_contents(d0);
			sleep(1);
			}
		}

	broker::done();
	return 0;
	}
