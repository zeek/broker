#ifndef ENDPOINTACTOR_HH
#define ENDPOINTACTOR_HH

#include <cppa/cppa.hpp>

#include "Subscription.hh"

namespace broker {

class EndpointActor : public cppa::sb_actor<EndpointActor> {
friend class cppa::sb_actor<EndpointActor>;

public:

	EndpointActor()
		{
		using namespace cppa;
		using namespace std;

		active = (
		on(atom("quit")) >> [=]
			{
			//aout(this) << "quit" << endl;
			quit();
			},
		on(atom("peer"), arg_match) >> [=](actor peer)
			{
			//aout(this) << "peer" << endl;

			sync_send(peer, atom("peer"), this, local_subs.Topics()).then(
				on_arg_match >> [=](sync_exited_msg m) { },
				on_arg_match >> [=](actor endpoint, Subscriptions topics)
					{
					demonitor(peer);
					monitor(peer);
					peers[peer.address()] = peer;
					peer_subs.AddSubscriber(Subscriber{move(topics), peer});

					if ( endpoint != peer )
						proxied[endpoint.address()] = peer;
					}
			);
			},
		on(atom("peer"), arg_match) >> [=](actor peer, Subscriptions t)
			{
			demonitor(peer);
			monitor(peer);
			peers[peer.address()] = peer;
			peer_subs.AddSubscriber(Subscriber{move(t), move(peer)});
			return make_cow_tuple(this, local_subs.Topics());
			},
		on(atom("unpeer"), arg_match) >> [=](actor peer)
			{
			//aout(this) << "unpeer" << endl;
			demonitor(peer);
			peers.erase(peer.address());
			peer_subs.RemSubscriber(peer.address());
			RemoveProxy(peer.address());
			},
		on_arg_match >> [=](down_msg d)
			{
			// Message is either from a local subscriber or a peer endpoint
			// (the peer may be a proxy to a remote actor).
			//aout(this) << "down" << endl;
			demonitor(d.source);
			peers.erase(d.source);
			peer_subs.RemSubscriber(d.source);
			RemoveProxy(d.source);
			Subscriptions unsubs = local_subs.RemSubscriber(d.source);

			for ( const auto& peer : peers )
				send(peer.second, atom("unsub"), unsubs, this);
			},
		on(atom("unsub"), arg_match) >> [=](Subscriptions topics, actor peer)
			{
			peer_subs.RemSubscriptions(topics, EndpointOrProxy(peer).address());
			},
		on(atom("sub"), arg_match) >> [=](SubscriptionTopic st, actor subscriber)
			{
			demonitor(subscriber);
			monitor(subscriber);
			Subscriptions subs;
			subs[+st.type].insert(st.topic);
			local_subs.AddSubscriber(Subscriber{move(subs), move(subscriber)});

			for ( const auto& p : peers )
				send(p.second, atom("subpeer"), st, this);
			},
		on(atom("subpeer"), arg_match) >> [=](SubscriptionTopic t, actor peer)
			{
			peer_subs.AddSubscription(move(t), EndpointOrProxy(peer));
			},
		on<SubscriptionTopic, anything>() >> [=](SubscriptionTopic st)
			{
			Publish(st, last_dequeued(), last_sender());
			}
		);
		}

private:

	void Publish(const SubscriptionTopic& topic, const cppa::any_tuple& msg,
	             const cppa::actor_addr& originator)
		{
		for ( const auto& a : local_subs.Match(topic) )
			send_tuple(a, msg);

		if ( peers.find(EndpointOrProxy(originator)) != peers.end() )
			// Don't re-publish messages published by a peer.
			return;

		for ( const auto& a : peer_subs.Match(topic) )
			send_tuple(a, msg);
		}

	bool IsProxied(const cppa::actor_addr& aa)
		{
		return proxied.find(aa) != proxied.end();
		}

	cppa::actor EndpointOrProxy(const cppa::actor& a)
		{
		auto it = proxied.find(a.address());
		return it == proxied.end() ? a : it->second;
		}

	cppa::actor_addr EndpointOrProxy(const cppa::actor_addr& a)
		{
		auto it = proxied.find(a);
		return it == proxied.end() ? a : it->second.address();
		}

	bool RemoveProxy(const cppa::actor_addr& proxy)
		{
		for ( ProxyMap::const_iterator it = proxied.begin();
		      it != proxied.end(); ++it )
			if ( it->second.address() == proxy )
				{
				proxied.erase(it);
				return true;
				}

		return false;
		}

	cppa::behavior active;
	cppa::behavior& init_state = active;

	ActorMap peers;
	using ProxyMap = std::map<cppa::actor_addr, cppa::actor>;
	ProxyMap proxied;  // maps remote endpoint actor to local peer proxy actor.
	SubscriberBase local_subs;
	SubscriberBase peer_subs;
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
			// assert(last_sender() != remote.address());
			//aout(this) << "to_remote: " << last_dequeued() << std::endl;
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
		send(local, atom("peer"), this);
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
