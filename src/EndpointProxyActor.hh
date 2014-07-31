#ifndef BROKER_ENDPOINTPROXYACTOR_HH
#define BROKER_ENDPOINTPROXYACTOR_HH

#include <caf/sb_actor.hpp>
#include <caf/io/remote_actor.hpp>

namespace broker {

/**
 * Manages connection to a remote EndpointActor including auto-reconnection
 * and associated peer/unpeer messages.
 */
class EndpointProxyActor : public caf::sb_actor<EndpointProxyActor> {
friend class caf::sb_actor<EndpointProxyActor>;

public:

	EndpointProxyActor(caf::actor local, std::string addr, uint16_t port,
	                   std::chrono::duration<double> retry_freq)
		{
		using namespace caf;
		using namespace std;

		bootstrap = (
		after(chrono::seconds(0)) >> [=]
			{
			try_connect(addr, port, local);
			}
		);

		disconnected = (
		on(atom("quit")) >> [=]
			{
			quit();
			},
		after(retry_freq) >> [=]
			{
			try_connect(addr, port, local);
			}
		);

		connected = (
		on(atom("quit")) >> [=]
			{
			send(local, atom("unpeer"), remote);
			quit();
			},
		on(atom("connwait")) >> [=]
			{
			return atom("ok");
			},
		on_arg_match >> [=](down_msg d)
			{
			demonitor(remote);
			remote = invalid_actor;
			become(disconnected);
			},
		others() >> [=]
			{
			// Proxy just maintains the peering relationship between
			// two endpoints, shouldn't be getting any messages itself.
			aout(this) << "ERROR, proxy got msg: " << last_dequeued() << endl;
			}
		);
		}

private:

	bool try_connect(const std::string& addr, uint16_t port,
	                 const caf::actor& local)
		{
		using namespace caf;
		using namespace std;

		try
			{
			remote = io::remote_actor(addr, port);
			}
		catch ( const exception& e )
			{
			// TODO: need better debug logging facilities
			//aout(this) << "Failed to connect to remote endpoint (" << addr
			//           << ", " << port << ")" << endl;
			}

		if ( ! remote )
			{
			become(disconnected);
			return false;
			}

		monitor(remote);
		become(connected);
		send(local, atom("peer"), remote);
		return true;
		}

	caf::actor remote = caf::invalid_actor;
	caf::behavior bootstrap;
	caf::behavior disconnected;
	caf::behavior connected;
	caf::behavior& init_state = bootstrap;
};

} // namespace broker

#endif // BROKER_ENDPOINTPROXYACTOR_HH
