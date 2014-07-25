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

using namespace std;

void lookup_cb(broker::data::Key key, unique_ptr<broker::data::Val> val,
               void* cookie)
	{
	// TODO
	}

void haskey_cb(broker::data::Key key, bool exists, void* cookie)
	{
	// TODO
	}

void keys_cb(std::unordered_set<broker::data::Key> keys, void* cookie)
	{
	// TODO
	}

void size_cb(uint64_t size, void* cookie)
	{
	// TODO
	}

int main(int argc, char** argv)
	{
	broker::init();

	if ( argc == 1 )
		{
		broker::Endpoint node{"node0"};

		broker::data::Master d0{node, "my_data"};
		broker::data::Master d1{node, "my_other_data"};

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
			}
		}
	else
		{
		broker::Endpoint node{argv[1]};

		node.AddPeer("localhost", 9999).BlockUntilConnected();

		broker::data::Clone d0{node, "my_data"};
		broker::data::Facade d1{node, "my_other_data"};

		string ck{"mycookie"};

		if ( false )
			{
			// Just demonstrating that the interface...
			d0.Insert("key0", "blah"); // will be applied to remote master.
			d1.Insert("key0", "abc");  // also applied to remote master.

			auto val = d0.Lookup("key0"); // get from local clone.
			d1.Lookup("key0", lookup_cb, &ck);  // queries remote store

			auto exists = d0.HasKey("key0"); // get from local clone.
			d1.HasKey("key0", haskey_cb, &ck);  // queries remote store

			d0.Erase("key0");
			d1.Erase("key0");

			d0.Clear(); // applied to remote master.
			d1.Clear(); // applied to remote master.

			auto keys = d0.Keys(); // get from local clone.
			d1.Keys(keys_cb, &ck); // queries remote store

			auto sz = d0.Size(); // determined from local clone
			d1.Size(size_cb, &ck); // queries remote store
			}

		for ( auto i = 0; ; ++i )
			{
			stringstream ss;
			ss << i;
			d0.Insert(node.Name(), ss.str());
			sleep(1);
			}
		}

	broker::done();
	return 0;
	}
