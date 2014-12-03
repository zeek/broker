#ifndef BROKER_ENDPOINT_IMPL_HH
#define BROKER_ENDPOINT_IMPL_HH

#include "broker/endpoint.hh"
#include "broker/peer_status.hh"
#include "broker/report.hh"
#include "broker/store/identifier.hh"
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
#include <sstream>

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

			for ( auto& sub : s->subscriptions )
				unadvertise_subscription(topic{move(sub.first)});
			},
		on(atom("unsub"), arg_match) >> [=](const topic& t, const actor& p)
			{
			peer_subscriptions.unregister_topic(t, p.address());
			},
		on(atom("sub"), arg_match) >> [=](topic& t, actor& p)
			{
			peer_subscriptions.register_topic(move(t), move(p));
			},
		on(atom("master"), arg_match) >> [=](store::identifier& id, actor& a)
			{
			if ( local_subscriptions.exact_match(id) )
				{
				ostringstream msg;
				msg << "Failed to register master data store with id '" << id
				    << "' because a master already exists with that id.";
				report::error("endpoint." + name + ".data.master." + id,
				              msg.str());
				return;
				}

			attach(move(id), move(a));
			},
		on(atom("local sub"), arg_match) >> [=](topic& t, actor& a)
			{
			attach(move(t), move(a));
			},
		on_arg_match >> [=](const topic& t, broker::message& msg,
		                    int flags)
			{
			bool from_peer = peers.find(last_sender()) != peers.end();

			if ( from_peer )
				{
				publish_locally(t, std::move(msg), flags, from_peer);
				// Don't re-publish messages sent by a peer (they go one hop).
				}
			else
				{
				publish_locally(t, msg, flags, from_peer);
				publish_current_msg_to_peers(t, flags);
				}
			},
		on(atom("storeactor"), arg_match) >> [=](const store::identifier& n)
			{
			return find_master(n);
			},
		on_arg_match >> [=](const store::identifier& n, const store::query& q,
		                    const actor& requester)
			{
			auto master = find_master(n);

			if ( master )
				forward_to(master);
			else
				send(requester, this,
				     store::result(store::result::status::failure));
			},
		on<store::identifier, anything>() >> [=](const store::identifier& id)
			{
			// This message should be a store update operation.
			auto master = find_master(id);

			if ( master )
				forward_to(master);
			else
				{
				ostringstream msg;
				msg << "Data store update dropped due to no existing master"
				    << "with id '" << id << "'";
				report::warn("endpoint." + name + ".data.master." + id,
				             msg.str());
				}
			},
		on(atom("flags"), arg_match) >> [=](int flags)
			{
			bool auto_sub_before = (behavior_flags & AUTO_ADVERTISE);
			behavior_flags = flags;
			bool auto_sub_after = (behavior_flags & AUTO_ADVERTISE);

			if ( auto_sub_before == auto_sub_after )
				return;

			if ( auto_sub_before )
				{
				topic_set to_remove;

				for ( const auto& t : advertised_subscriptions )
					if ( advert_acls.find(t.first) == advert_acls.end() )
						to_remove.insert({t.first, true});

				for ( const auto& t : to_remove )
					unadvertise_subscription(topic{t.first});

				return;
				}

			for ( const auto& t : local_subscriptions.topics() )
				advertise_subscription(topic{t.first});
			},
		on(caf::atom("acl pub"), arg_match) >>[=](topic& t)
			{
			pub_acls.insert({move(t), true});
			},
		on(caf::atom("acl unpub"), arg_match) >>[=](const topic& t)
			{
			pub_acls.erase(t);
			},
		on(caf::atom("advert"), arg_match) >>[=](string& t)
			{
			if ( advert_acls.insert({t, true}).second &&
			     local_subscriptions.exact_match(t) )
				// Now permitted to advertise an existing subscription.
				advertise_subscription(move(t));
			},
		on(caf::atom("unadvert"), arg_match) >>[=](string& t)
			{
			if ( advert_acls.erase(t) && local_subscriptions.exact_match(t) )
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

	void attach(std::string topic_or_id, caf::actor a)
		{
		demonitor(a);
		monitor(a);
		local_subscriptions.register_topic(topic_or_id, std::move(a));

		if ( (behavior_flags & AUTO_ADVERTISE) ||
		     advert_acls.find(topic_or_id) != advert_acls.end() )
			advertise_subscription(std::move(topic_or_id));
		}

	caf::actor find_master(const store::identifier& id)
		{
		auto m = local_subscriptions.exact_match(id);

		if ( ! m )
			m = peer_subscriptions.exact_match(id);

		if ( ! m )
			return caf::invalid_actor;

		return *m->begin();
		}

	void advertise_subscription(topic t)
		{
		if ( advertised_subscriptions.insert({t, true}).second )
			publish_subscription_operation(std::move(t), caf::atom("sub"));
		}

	void unadvertise_subscription(topic t)
		{
		if ( advertised_subscriptions.erase(t) )
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

	void publish_locally(const topic& t, broker::message msg, int flags,
	                     bool from_peer)
		{
		if ( ! from_peer && ! (flags & SELF) )
			return;

		auto matches = local_subscriptions.prefix_matches(t);

		if ( matches.empty() )
			return;

		auto caf_msg = caf::make_message(std::move(msg));

		for ( const auto& match : matches )
			for ( const auto& a : match->second )
				send_tuple(a, caf_msg);
		}

	void publish_current_msg_to_peers(const topic& t, int flags)
		{
		if ( ! (flags & PEERS) )
			return;

		if ( ! (behavior_flags & AUTO_PUBLISH) &&
		     pub_acls.find(t) == pub_acls.end() )
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
	topic_set advert_acls;

	std::unordered_map<caf::actor_addr, peer_endpoint> peers;
	subscription_registry local_subscriptions;
	subscription_registry peer_subscriptions;
	topic_set advertised_subscriptions;
};

/**
 * Manages connection to a remote endpoint_actor including auto-reconnection
 * and associated peer/unpeer messages.
 */
class endpoint_proxy_actor : public caf::sb_actor<endpoint_proxy_actor> {
friend class caf::sb_actor<endpoint_proxy_actor>;

public:

	endpoint_proxy_actor(caf::actor local, std::string endpoint_name,
	                     std::string addr, uint16_t port,
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
			try_connect(pi, endpoint_name);
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
		after(chrono::duration_cast<chrono::microseconds>(retry_freq)) >> [=]
			{
			try_connect(pi, endpoint_name);
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
			ostringstream st;
			st << "endpoint." << endpoint_name << ".remote_proxy." << addr
			   << ":" << port;
			ostringstream msg;
			msg << "Remote endpoint proxy got msg: "
			    << caf::to_string(last_dequeued());
			report::warn(st.str(), msg.str());
			}
		);
		}

private:

	bool try_connect(const peering::impl& pi, const std::string& endpoint_name)
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
			ostringstream st;
			st << "endpoint." << endpoint_name << ".remote_proxy." << addr
			   << ":" << port;
			ostringstream msg;
			msg << "Failed to connect to remote endpoint: " << e.what();
			report::warn(st.str(), msg.str());
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
