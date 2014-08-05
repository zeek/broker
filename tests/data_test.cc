#include "broker/broker.hh"
#include "broker/Endpoint.hh"
#include "broker/Peer.hh"
#include "broker/data/Facade.hh"
#include "broker/data/Clone.hh"
#include "broker/data/Master.hh"
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

void lookup_cb(broker::data::Key key, unique_ptr<broker::data::Val> val,
               void* cookie)
	{
	lock_guard<mutex> lock(*static_cast<mutex*>(cookie));
	printf("lookup_cb: %s -> %s\n", key.c_str(), val ? val.get()->c_str()
	                                                 : "null");
	}

void haskey_cb(broker::data::Key key, bool exists, void* cookie)
	{
	lock_guard<mutex> lock(*static_cast<mutex*>(cookie));
	printf("haskey_cb: %s -- %s\n", key.c_str(), exists ? "yes" : "no");
	}

void keys_cb(std::unordered_set<broker::data::Key> keys, void* cookie)
	{
	lock_guard<mutex> lock(*static_cast<mutex*>(cookie));
	printf("keys_cb:\n");
	for ( const auto& k : keys )
		printf("\t%s\n", k.c_str());
	}

void size_cb(uint64_t size, void* cookie)
	{
	lock_guard<mutex> lock(*static_cast<mutex*>(cookie));
	printf("size_cb: %" PRIu64 "\n", size);
	}

static void dump_store_contents(const broker::data::Facade& store)
	{
	string header = "==== " + store.Topic() + " Contents ====";
	std::stringstream ss;
	ss << header << std::endl;

	auto keys = store.Keys();

	for ( const auto& key : keys )
		{
		auto val = store.Lookup(key);
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

	if ( argc == 1 )
		{
		broker::Endpoint node{"node0"};

		broker::data::Master d0{node, "my_data"};

		if ( ! node.Listen(9999, "127.0.0.1") )
			{
			cerr << node.LastError() << endl;
			return 1;
			}

		for ( auto i = 0; ; ++i )
			{
			stringstream ss;
			ss << i;
			d0.Insert(node.Name(), ss.str());

			sleep(1);
			dump_store_contents(d0);
			sleep(1);
			}
		}
	else
		{
		broker::Endpoint node{argv[1]};

		node.AddPeer("localhost", 9999).BlockUntilConnected();

		broker::data::Clone d0{node, "my_data"};
		broker::data::Facade d1{node, "my_data"};

		if ( false )
			{
			// Just demonstrating the interface...
			d0.Insert("key0", "blah"); // will be applied to remote master.
			d1.Insert("key0", "abc");  // also applied to remote master.

			auto val = d0.Lookup("key0"); // get from local clone.
			d1.Lookup("key0", lookup_cb, &mtx);  // queries remote store

			auto exists = d0.HasKey("key0"); // get from local clone.
			d1.HasKey("key0", haskey_cb, &mtx);  // queries remote store

			d0.Erase("key0"); // applied to remote master.
			d1.Erase("key0"); // applied to remote master.

			d0.Clear(); // applied to remote master.
			d1.Clear(); // applied to remote master.

			auto keys = d0.Keys(); // get from local clone.
			d1.Keys(keys_cb, &mtx); // queries remote store

			auto sz = d0.Size(); // determined from local clone
			d1.Size(size_cb, &mtx); // queries remote store
			}

		for ( auto i = 0; ; ++i )
			{
			stringstream ss;
			ss << i;
			d0.Insert(node.Name(), ss.str());

			auto sz = d0.Size();
			printf("size: %" PRIu64 "\n", sz);
			sz = d1.Size();
			printf("size: %" PRIu64 "\n", sz);
			d0.Size(size_cb, &mtx);
			d1.Size(size_cb, &mtx);

			auto exists = d0.HasKey(node.Name());
			printf("key '%s' exists: %s\n", node.Name().c_str(),
			       exists ? "yes" : "no");
			exists = d1.HasKey(node.Name());
			printf("key '%s' exists: %s\n", node.Name().c_str(),
			       exists ? "yes" : "no");
			exists = d1.HasKey("nope");
			printf("key '%s' exists: %s\n", "nope", exists ? "yes" : "no");
			d0.HasKey("nope", haskey_cb, &mtx);
			d1.HasKey(node.Name(), haskey_cb, &mtx);

			auto val = d0.Lookup(node.Name());
			printf("lookup: %s -> %s\n", node.Name().c_str(),
			       val ? val.get()->c_str() : "null");
			val = d1.Lookup(node.Name());
			printf("lookup: %s -> %s\n", node.Name().c_str(),
			       val ? val.get()->c_str() : "null");
			d0.Lookup("nope", lookup_cb, &mtx);
			d1.Lookup(node.Name(), lookup_cb, &mtx);

			sleep(1);
			dump_store_contents(d0);
			sleep(1);
			}
		}

	broker::done();
	return 0;
	}
