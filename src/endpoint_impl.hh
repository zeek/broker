#ifndef BROKER_ENDPOINT_IMPL_HH
#define BROKER_ENDPOINT_IMPL_HH

#include "broker/endpoint.hh"
#include "broker/peer_status.hh"
#include "broker/store/store.hh"
#include "broker/store/query.hh"
#include "subscription.hh"
#include "peering_impl.hh"
#include "util/radix_tree.hh"
#include <caf/actor.hpp>
#include <caf/spawn.hpp>
#include <caf/send.hpp>
#include <caf/sb_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/io/remote_actor.hpp>
#include <unordered_set>

namespace broker {

static void do_peer_status(const caf::actor q, peering::impl pi,
                           peer_status::tag t, std::string pname = "")
	{
	peering p{std::unique_ptr<peering::impl>(new peering::impl(std::move(pi)))};
	caf::anon_send(q, peer_status{std::move(p), t, std::move(pname)});
	}

class endpoint_actor : public caf::sb_actor<endpoint_actor> {
friend class caf::sb_actor<endpoint_actor>;

public:

	endpoint_actor(std::string name, int flags, caf::actor peer_status_q)
		: behavior_flags(flags)
		{
		using namespace caf;
		using namespace std;
		using namespace broker::store;

		active = (
		on_arg_match >> [=](int version)
			{
			return make_message(BROKER_PROTOCOL_VERSION == version,
			                    BROKER_PROTOCOL_VERSION);
			},
		on(atom("peer"), arg_match) >> [=](actor& p, peering::impl& pi)
			{
			auto it = peers.find(p.address());

			if ( it != peers.end() )
				{
				do_peer_status(peer_status_q, move(pi),
				               peer_status::tag::established, it->second.name);
				return;
				}

			sync_send(p, BROKER_PROTOCOL_VERSION).then(
				on_arg_match >> [=](const sync_exited_msg& m)
					{
					do_peer_status(peer_status_q, move(pi),
					               peer_status::tag::disconnected);
					},
				on_arg_match >> [=](bool compat, int their_version)
					{
					if ( ! compat )
						do_peer_status(peer_status_q, move(pi),
						               peer_status::tag::incompatible);
					else
						sync_send(p, atom("peer"), this, name,
						          advertised_subscriptions).then(
							on_arg_match >> [=](const sync_exited_msg& m)
								{
								do_peer_status(peer_status_q, move(pi),
								               peer_status::tag::disconnected);
								},
							on_arg_match >> [=](string& pname, topic_set& ts)
								{
								add_peer(move(p), pname, move(ts));
								do_peer_status(peer_status_q, move(pi),
								               peer_status::tag::established,
								               move(pname));
								}
						);
					},
				others() >> [=]
					{
					do_peer_status(peer_status_q, move(pi),
					               peer_status::tag::incompatible);
					}
			);
			},
		on(atom("peer"), arg_match) >> [=](actor& p, string& pname,
		                                   topic_set& ts)
			{
			add_peer(move(p), move(pname), move(ts));
			return make_message(name, advertised_subscriptions);
			},
		on(atom("unpeer"), arg_match) >> [=](const actor& p)
			{
			demonitor(p);
			peers.erase(p.address());
			peer_subscriptions.erase(p.address());
			},
		on_arg_match >> [=](const down_msg& d)
			{
			demonitor(d.source);

			if ( peers.erase(d.source) )
				{
				peer_subscriptions.erase(d.source);
				return;
				}

			auto s = local_subscriptions.erase(d.source);

			if ( ! s )
				return;

			for ( auto i = 0; i < s->subscriptions.size(); ++i )
				for ( auto& sub : s->subscriptions[i] )
					if ( advertised_subscriptions[i].find(sub.first) !=
					     advertised_subscriptions[i].end() )
						unadvertise_subscription({move(sub.first),
						                         static_cast<topic::tag>(i)});
			},
		on(atom("unsub"), arg_match) >> [=](const topic& t, const actor& p)
			{
			peer_subscriptions.unregister_topic(t, p.address());
			},
		on(atom("sub"), arg_match) >> [=](topic& t, actor& p)
			{
			peer_subscriptions.register_topic(move(t), move(p));
			},
		on(atom("local sub"), arg_match) >> [=](topic& t, actor& a)
			{
			demonitor(a);
			monitor(a);
			local_subscriptions.register_topic(t, move(a));

			if ( (behavior_flags & AUTO_SUBSCRIBE) ||
			     sub_acls[+t.type].find(t.name) != sub_acls[+t.type].end() )
				advertise_subscription(move(t));
			},
		on<topic, int, anything>() >> [=](const topic& t, int flags)
			{
			bool from_peer = peers.find(last_sender()) != peers.end();
			publish_current_msg_locally(t, flags, from_peer);

			if ( from_peer )
				// Don't re-publish messages sent by a peer (they go one hop).
				return;

			publish_current_msg_to_peers(t, flags);
			},
		on_arg_match >> [=](const topic& t, const query& q,
		                    const actor& requester)
			{
			auto master = find_master(t);

			if ( master )
				forward_to(master);
			else
				send(requester, this, result(result::status::failure));
			},
		on<topic, anything>() >> [=](const topic& t)
			{
			// This message should be a store update operation.
			auto master = find_master(t);

			if ( master )
				forward_to(master);
			// TODO: else emit an error
			},
		on(atom("flags"), arg_match) >> [=](int flags)
			{
			bool auto_sub_before = (behavior_flags & AUTO_SUBSCRIBE);
			behavior_flags = flags;
			bool auto_sub_after = (behavior_flags & AUTO_SUBSCRIBE);

			if ( auto_sub_before == auto_sub_after )
				return;

			if ( auto_sub_before )
				{
				topic_set to_remove;

				for ( auto i = 0; i < advertised_subscriptions.size(); ++i )
					for ( const auto& t : advertised_subscriptions[i] )
						if ( sub_acls[i].find(t.first) == sub_acls[i].end() )
							to_remove[i].insert({t.first, true});

				for ( auto i = 0; i < to_remove.size(); ++i )
					for ( const auto& t : to_remove[i] )
						unadvertise_subscription({t.first,
						                          static_cast<topic::tag>(i)});
				return;
				}

			for ( auto i = 0; i < local_subscriptions.topics().size(); ++i )
				for ( const auto& t : local_subscriptions.topics()[i] )
					if ( advertised_subscriptions[i].find(t.first) ==
					     advertised_subscriptions[i].end() )
					advertise_subscription({t.first,
					                        static_cast<topic::tag>(i)});
			},
		on(caf::atom("acl pub"), arg_match) >>[=](topic& t)
			{
			pub_acls[+t.type].insert({move(t.name), true});
			},
		on(caf::atom("acl unpub"), arg_match) >>[=](const topic& t)
			{
			pub_acls[+t.type].erase(t.name);
			},
		on(caf::atom("acl sub"), arg_match) >>[=](topic& t)
			{
			if ( sub_acls[+t.type].insert({t.name, true}).second &&
			     local_subscriptions.exact_match(t) )
				// Now permitted to advertise an existing subscription.
				advertise_subscription(move(t));
			},
		on(caf::atom("acl unsub"), arg_match) >>[=](topic& t)
			{
			if ( sub_acls[+t.type].erase(t.name) &&
			     local_subscriptions.exact_match(t) )
				// No longer permitted to advertise an existing subscription.
				unadvertise_subscription(move(t));
			}
		);
		}

private:

	void add_peer(caf::actor p, std::string name, topic_set ts)
		{
		demonitor(p);
		monitor(p);
		peers[p.address()] = {p, std::move(name)};
		peer_subscriptions.insert(subscriber{std::move(p), std::move(ts)});
		}

	caf::actor find_master(const topic& t)
		{
		auto m = local_subscriptions.exact_match(t);

		if ( ! m )
			m = peer_subscriptions.exact_match(t);

		if ( ! m )
			return caf::invalid_actor;

		return *m->begin();
		}

	void advertise_subscription(topic t)
		{
		if ( advertised_subscriptions[+t.type].insert({t.name, true}).second )
			publish_subscription_operation(std::move(t), caf::atom("sub"));
		}

	void unadvertise_subscription(topic t)
		{
		if ( advertised_subscriptions[+t.type].erase(t.name) )
			publish_subscription_operation(std::move(t), caf::atom("unsub"));
		}

	void publish_subscription_operation(topic t, caf::atom_value op)
		{
		if ( peers.empty() )
			return;

		auto msg = caf::make_message(std::move(op), std::move(t), this);

		for ( const auto& p : peers )
			send_tuple(p.second.ep, msg);
		}

	void publish_current_msg_locally(const topic& t, int flags,
	                                 bool from_peer)
		{
		if ( ! from_peer && ! (flags & SELF) )
			return;

		for ( const auto& match : local_subscriptions.prefix_matches(t) )
			for ( const auto& a : match->second )
				forward_to(a);
		}

	void publish_current_msg_to_peers(const topic& t, int flags)
		{
		if ( ! (flags & PEERS) )
			return;

		if ( ! (behavior_flags & AUTO_PUBLISH) &&
		     pub_acls[+t.type].find(t.name) == pub_acls[+t.type].end() )
			// Not allowed to publish this topic to peers.
			return;

        // send_tuple instead of forward_to so peer can use
        // last_sender() to check if msg comes from a peer.
        if ( (flags & UNSOLICITED) )
            for ( const auto& p : peers )
                send_tuple(p.second.ep, last_dequeued());
        else
            for ( const auto& a : peer_subscriptions.unique_prefix_matches(t) )
                send_tuple(a, last_dequeued());
		}

	struct peer_endpoint {
		caf::actor ep;
		std::string name;
	};

	caf::behavior active;
	caf::behavior& init_state = active;

	int behavior_flags;
	topic_set pub_acls;
	topic_set sub_acls;
	topic_set advertised_subscriptions;

	std::unordered_map<caf::actor_addr, peer_endpoint> peers;
	subscription_registry local_subscriptions;
	subscription_registry peer_subscriptions;
};

/**
 * Manages connection to a remote endpoint_actor including auto-reconnection
 * and associated peer/unpeer messages.
 */
class endpoint_proxy_actor : public caf::sb_actor<endpoint_proxy_actor> {
friend class caf::sb_actor<endpoint_proxy_actor>;

public:

	endpoint_proxy_actor(caf::actor local, std::string addr, uint16_t port,
	                     std::chrono::duration<double> retry_freq,
	                     caf::actor peer_status_q)
		{
		using namespace caf;
		using namespace std;
		peering::impl pi(local, this, true, make_pair(addr, port));

		trap_exit(true);

		bootstrap = (
		after(chrono::seconds(0)) >> [=]
			{
			try_connect(pi);
			}
		);

		disconnected = (
		on(atom("peerstat")) >> [=]
			{
			do_peer_status(peer_status_q, pi, peer_status::tag::disconnected);
			},
		on_arg_match >> [=](const exit_msg& e)
			{
			quit();
			},
		on(atom("quit")) >> [=]
			{
			quit();
			},
		after(retry_freq) >> [=]
			{
			try_connect(pi);
			}
		);

		connected = (
		on(atom("peerstat")) >> [=]
			{
			send(local, atom("peer"), remote, pi);
			},
		on_arg_match >> [=](const exit_msg& e)
			{
			send(remote, atom("unpeer"), local);
			send(local, atom("unpeer"), remote);
			quit();
			},
		on(atom("quit")) >> [=]
			{
			send(remote, atom("unpeer"), local);
			send(local, atom("unpeer"), remote);
			quit();
			},
		on_arg_match >> [=](const down_msg& d)
			{
			demonitor(remote);
			remote = invalid_actor;
			become(disconnected);
			do_peer_status(peer_status_q, pi, peer_status::tag::disconnected);
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

	bool try_connect(const peering::impl& pi)
		{
		using namespace caf;
		using namespace std;
		const std::string& addr = pi.remote_tuple.first;
		const uint16_t& port = pi.remote_tuple.second;
		const caf::actor& local = pi.endpoint_actor;

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
		send(local, atom("peer"), remote, pi);
		return true;
		}

	caf::actor remote = caf::invalid_actor;
	caf::behavior bootstrap;
	caf::behavior disconnected;
	caf::behavior connected;
	caf::behavior& init_state = bootstrap;
};

static inline caf::actor& handle_to_actor(void* h)
	{ return *static_cast<caf::actor*>(h); }

class endpoint::impl {
public:

	impl(std::string n, int arg_flags)
		: name(std::move(n)), flags(arg_flags), self(), peer_status(),
		  actor(caf::spawn<broker::endpoint_actor>(name, flags,
		                   handle_to_actor(peer_status.handle()))),
		  peers(), last_errno(), last_error()
		{
		self->planned_exit_reason(caf::exit_reason::user_defined);
		actor->link_to(self);
		}

	std::string name;
	int flags;
	caf::scoped_actor self;
	peer_status_queue peer_status;
	caf::actor actor;
	std::unordered_set<peering> peers;
	int last_errno;
	std::string last_error;
};
} // namespace broker

#endif // BROKER_ENDPOINT_IMPL_HH
