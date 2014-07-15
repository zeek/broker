#ifndef ENDPOINTACTOR_HH
#define ENDPOINTACTOR_HH

#include <cppa/cppa.hpp>

#include <unordered_map>

namespace broker {

class EndpointActor : public cppa::sb_actor<EndpointActor> {
friend class cppa::sb_actor<EndpointActor>;

public:

	EndpointActor()
		{
		using namespace cppa;

		active = (
		on(atom("quit")) >> [=]
			{
			aout(this) << "quit" << std::endl;
			quit();
			},
		on(atom("peer"), arg_match) >> [=](actor peer)
			{
			aout(this) << "peer" << std::endl;
			monitor(peer);
			peers[peer.address()] = peer;
			},
		on(atom("unpeer"), arg_match) >> [=](actor peer)
			{
			aout(this) << "unpeer" << std::endl;
			demonitor(peer);
			peers.erase(peer.address());
			},
		on_arg_match >> [=](down_msg d)
			{
			aout(this) << "down" << std::endl;
			demonitor(d.source);
			peers.erase(d.source);
			},
		others() >> [=]
			{
			aout(this) << "got msg: " << last_dequeued() << std::endl;
			},
		// TODO: remove this heartbeat; it's just for debugging
		after(std::chrono::seconds(5)) >> [=]
			{
			aout(this) << "begin sending heartbeats" << std::endl;
			for ( auto p : peers )
				{
				aout(this) << "\tsent heartbeat" << std::endl;
				send(p.second, atom("heartbeat"));
				}
			aout(this) << "done sending heartbeats" << std::endl;
			}
		);
		}

private:

	cppa::behavior active;
	cppa::behavior& init_state = active;

	std::unordered_map<cppa::actor_addr, cppa::actor> peers;
};

class RemoteEndpointActor : public cppa::sb_actor<RemoteEndpointActor> {
friend class cppa::sb_actor<RemoteEndpointActor>;

public:

	RemoteEndpointActor(cppa::actor local, std::string addr, uint16_t port,
	                    std::chrono::duration<double> retry_freq)
		{
		using namespace cppa;
		using namespace std;

		partial_function common {
		on(atom("quit")) >> [=]
			{
			quit();
			}
		};

		bootstrap = (
		after(chrono::seconds(0)) >> [=]
			{
			try_connect(addr, port, local);
			}
		);

		disconnected = common.or_else(
		after(retry_freq) >> [=]
			{
			try_connect(addr, port, local);
			}
		);

		connected = common.or_else(
		on_arg_match >> [=](down_msg d)
			{
			demonitor(remote);
			remote = invalid_actor;
			become(disconnected);
			},
		others() >> [=]
			{
			// Remote side should be sending messages directly to local
			// endpoint, not to this proxy actor.
			// assert last_sender() != remote.address()
			aout(this) << "to_remote: " << last_dequeued() << std::endl;
			forward_to(remote);
			}
		);
		}

private:

	bool try_connect(const std::string& addr, uint16_t port,
	                 const cppa::actor& local)
		{
		using namespace cppa;
		using namespace std;

		try
			{
			remote = remote_actor(addr, port);
			}
		catch ( const exception& e )
			{
			// TODO: need better debug logging facilities
			aout(this) << "Failed to connect to remote endpoint (" << addr
			           << ", " << port << ")" << endl;
			}

		if ( ! remote )
			{
			become(disconnected);
			return false;
			}

		monitor(remote);
		become(connected);
		send(remote, atom("peer"), local);
		return true;
		}

	cppa::actor remote = cppa::invalid_actor;
	cppa::behavior bootstrap;
	cppa::behavior disconnected;
	cppa::behavior connected;
	cppa::behavior& init_state = bootstrap;
};

} // namespace broker

#endif // ENDPOINTACTOR_HH
