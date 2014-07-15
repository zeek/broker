#include "broker/broker.hh"
#include "broker/Endpoint.hh"
#include <cstdio>
#include <string>
#include <iostream>
#include <unistd.h>

using namespace std;

struct bro_key;
struct bro_val;

int main(int argc, char** argv)
	{
	broker::init();

	if ( argc > 1)
		{
		broker::Endpoint n0{"node0"};

		if ( ! n0.Listen(9999, "127.0.0.1") )
			{
			cerr << n0.LastError() << endl;
			return 1;
			}

		for ( ; ; )
			{
			sleep(1);
			}
		}
	else
		{
		broker::Endpoint n1{"node1"};
		broker::Peer remote = n1.AddPeer("localhost", 9999);

		for ( ; ; )
			{
			sleep(1);
			/*
			broker::Endpoint n2{"node2"};
			broker::Peer local = n1.AddPeer(n2);
			n1.RemPeer(local);
			*/
			}
		}

	broker::done();
	return 0;
	}
