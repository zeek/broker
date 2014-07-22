#include "broker/broker.hh"
#include "broker/Endpoint.hh"
#include "broker/PrintHandler.hh"
#include <cstdio>
#include <string>
#include <iostream>
#include <unistd.h>
#include <mutex>

using namespace std;

static void print_cb(std::string topic, std::string msg, void* cookie)
	{
	lock_guard<mutex> lock(*static_cast<mutex*>(cookie));
	cout << "got print (" << topic << "): " << msg << endl;
	}

int main(int argc, char** argv)
	{
	broker::init();
	mutex mtx;

	if ( argc == 1 )
		{
		broker::Endpoint n0{"node0"};

		broker::PrintHandler ha{n0, "topic_a", print_cb, &mtx};
		broker::PrintHandler hc{n0, "topic_c", print_cb, &mtx};

		if ( ! n0.Listen(9999, "127.0.0.1") )
			{
			cerr << n0.LastError() << endl;
			return 1;
			}

		for ( ; ; )
			{
			sleep(1);
			n0.Print("topic_a", n0.Name() + "_ping_a");
			n0.Print("topic_b", n0.Name() + "_ping_b");
			n0.Print("topic_c", n0.Name() + "_ping_c");
			}
		}
	else
		{
		broker::Endpoint n1{argv[1]};
		auto peer = n1.AddPeer("localhost", 9999);
		//n1.RemPeer(peer);

		broker::PrintHandler hb{n1, "topic_b", print_cb, &mtx};
		broker::PrintHandler hc{n1, "topic_c", print_cb, &mtx};

		for ( ; ; )
			{
			sleep(1);
			n1.Print("topic_a", n1.Name() + "_ping_a");
			n1.Print("topic_b", n1.Name() + "_ping_b");
			n1.Print("topic_c", n1.Name() + "_ping_c");
			}
		}

	broker::done();
	return 0;
	}
