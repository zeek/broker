#include "broker/broker.hh"
#include <cstdio>
#include <string>
#include <functional>
#include <unordered_set>

using namespace std;

struct bro_key;
struct bro_val;

void my_print_cb(const string& topic, const string& msg, void* cookie)
	{
	printf("Got print for topic '%s': %.*s\n", topic.c_str(), msg.size(),
	       msg.data());
	}

int main(int argc, char** argv)
	{
	broker::init();

	if ( argc > 1)
		{
		broker::Endpoint n0{"node0"};

		broker::PrintHandler ph0{n0, "topic_a", my_print_cb, "mycookie"};
		broker::PrintHandler ph1{n0, "topic_c", my_print_cb};

		broker::data::Master d0{n0, "my_data"};
		broker::data::Master d1{n0, "my_other_data"};

		n0.Listen("localhost", 9999);

		for ( ; ; )
			{
			sleep(1);

			n0.Print("topic_a", "ping_a");  // Just ph0 in this proc.
			n0.Print("topic_b", "ping_b");  // Just ph0 in other proc.
			n0.Print("topic_c", "ping_c");  // ph1 in both procs.
			// Program may do other stuff now...
			}
		}
	else
		{
		broker::Endpoint n1{"node3"};

		broker::PrintHandler ph0{n1, "topic_b", my_print_cb};
		broker::PrintHandler ph1{n1, "topic_c", my_print_cb};

		broker::data::Clone d0{n1, "my_data"};
		broker::data::Facade d1{n1, "my_other_data"};

		n1.AddPeer("localhost", 9999); // peer w/ n0 in other process

		for ( ; ; )
			{
			sleep(1);

			d0.Insert("key0", "blah"); // will be applied to remote master.
			d1.Insert("key0", "abc");  // also applied to remote master.

			auto val = d0.Lookup("key0"); // get from local clone.
			d1.Lookup("key0", "mycookie",
				[](const bro_key& key, const bro_val& val, void* cookie)
				{
				// Do something with params.
				}); // from remote store since it's not cloned.

			d0.Erase("key0");
			d1.Erase("key0");

			d0.Clear(); // applied to remote master.
			d1.Clear(); // applied to remote master.

			auto keys = d0.Keys(); // get from local clone.

			d1.Keys("mycookie",
				[](const std::unordered_set<bro_key>& keys, void* cookie)
				{
				// Do something with params.
				}
			); // from remote store since it's not cloned.

			// Program may do other stuff now...
			}
		}

	broker::done();
	return 0;
	}
