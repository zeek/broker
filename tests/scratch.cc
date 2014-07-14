#include <broker/broker.h>
#include <cppa/cppa.hpp>
#include <cstdio>
#include <cstdint>
#include <string>

using namespace std;
using namespace cppa;

class local_endpoint : public sb_actor<local_endpoint> {
friend class sb_actor<local_endpoint>;

public:

	local_endpoint()
		{
		listening = (
		on(atom("quit")) >> [=]
			{
			quit();
			},
		others() >> [=]
			{
			aout(this) << "Got msg: '" << to_string(last_dequeued()) << "'"
			           << endl;
			}
		);
		}

private:

	behavior listening;
	behavior& init_state = listening;
};

class remote_endpoint : public sb_actor<remote_endpoint> {
friend class sb_actor<remote_endpoint>;

public:

	remote_endpoint(const string& addr, uint16_t port,
	                chrono::duration<double> retry_freq = chrono::seconds(5))
		{
		partial_function common {
			on(atom("quit")) >> [=]
				{
				quit();
				}
		};

		bootstrap = (
		after(chrono::seconds(0)) >> [=]
			{
			try_connect(addr, port);
			}
		);

		disconnected = common.or_else(
		after(retry_freq) >> [=]
			{
			try_connect(addr, port);
			}
		);

		connected = common.or_else(
		on_arg_match >> [=](down_msg& d)
			{
			demonitor(remote);
			remote = invalid_actor;
			become(disconnected);
			},
		others() >> [=]
			{
			forward_to(remote);
			}
		);
		}

private:

	bool try_connect(const string& addr, uint16_t port)
		{
		try
			{
			remote = remote_actor(addr, port);
			}
		catch ( exception& e )
			{
			// TODO: need better place for this debug output
			aout(this) << "Failed to connect to remote endpoint ("
					   << addr << ", " <<  port << "): " << e.what() << endl;
			}

		if ( ! remote )
			{
			become(disconnected);
			return false;
			}

		monitor(remote);
		become(connected);
		return true;
		}

	actor remote = invalid_actor;
	behavior bootstrap;
	behavior disconnected;
	behavior connected;
	behavior& init_state = bootstrap;
};

int main(int argc, char** argv)
	{
	if ( argc > 1 )
		{
		auto r = spawn<remote_endpoint>("127.0.0.1", 9999);

		for ( ; ; )
			{
			sleep(1);
			anon_send(r, atom("print"), "mytopic", "ping");
			}
		}
	else
		{
		auto e = spawn<local_endpoint>();
		publish(e, 9999, "127.0.0.1");
		//publish(e, 10000, "127.0.0.1");
		}

	await_all_actors_done();
	return 0;
	}
