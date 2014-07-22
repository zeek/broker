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
				on_arg_match >> [=](Subscriptions topics)
					{
					//aout(this) << "got peer response" << endl;
					demonitor(peer);
					monitor(peer);
					peers[peer.address()] = peer;
					peer_subs.AddSubscriber(Subscriber{move(topics), peer});
					}
			);
			},
		on(atom("peer"), arg_match) >> [=](actor peer, Subscriptions t)
			{
			//aout(this) << "send peer response" << endl;
			demonitor(peer);
			monitor(peer);
			peers[peer.address()] = peer;
			peer_subs.AddSubscriber(Subscriber{move(t), move(peer)});
			return make_cow_tuple(local_subs.Topics());
			},
		on(atom("unpeer"), arg_match) >> [=](actor peer)
			{
			//aout(this) << "unpeer" << endl;
			demonitor(peer);
			peers.erase(peer.address());
			peer_subs.RemSubscriber(peer.address());

			if ( peer.address() != last_sender() )
				send(peer, atom("unpeer"), this);
			},
		on_arg_match >> [=](down_msg d)
			{
			// Message is either from a local subscriber or a peer endpoint.
			//aout(this) << "down" << endl;
			demonitor(d.source);
			peers.erase(d.source);
			peer_subs.RemSubscriber(d.source);
			Subscriptions unsubs = local_subs.RemSubscriber(d.source);

			for ( const auto& peer : peers )
				send(peer.second, atom("unsub"), unsubs, this);
			},
		on(atom("unsub"), arg_match) >> [=](Subscriptions topics, actor peer)
			{
			peer_subs.RemSubscriptions(topics, peer.address());
			},
		on(atom("sub"), arg_match) >> [=](SubscriptionTopic t, actor subscriber)
			{
			demonitor(subscriber);
			monitor(subscriber);
			Subscriptions subs;
			subs[+t.type].insert(t.topic);
			local_subs.AddSubscriber(Subscriber{move(subs), move(subscriber)});

			for ( const auto& p : peers )
				send(p.second, atom("subpeer"), t, this);
			},
		on(atom("subpeer"), arg_match) >> [=](SubscriptionTopic t, actor peer)
			{
			peer_subs.AddSubscription(move(t), peer);
			},
		on<SubscriptionTopic, anything>() >> [=](SubscriptionTopic t)
			{
			PublishCurrentMsg(t);
			}
		);
		}

private:

	void PublishCurrentMsg(const SubscriptionTopic& topic)
		{
		for ( const auto& a : local_subs.Match(topic) )
			forward_to(a);

		if ( peers.find(last_sender()) != peers.end() )
			// Don't re-publish messages published by a peer (they go one hop).
			return;

		for ( const auto& a : peer_subs.Match(topic) )
			// Use send_tuple instead of forward_to for above re-publish check.
			send_tuple(a, last_dequeued());
		}

	cppa::behavior active;
	cppa::behavior& init_state = active;

	ActorMap peers;
	SubscriberBase local_subs;
	SubscriberBase peer_subs;
};

} // namespace broker

#endif // ENDPOINTACTOR_HH
