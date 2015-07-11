#ifndef BROKER_ENDPOINT_IMPL_HH
#define BROKER_ENDPOINT_IMPL_HH

#include "broker/endpoint.hh"
#include "broker/report.hh"
#include "broker/store/identifier.hh"
#include "broker/store/query.hh"
#include "subscription.hh"
#include "peering_impl.hh"
#include "util/radix_tree.hh"
#include "atoms.hh"
#include <caf/actor.hpp>
#include <caf/spawn.hpp>
#include <caf/send.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/io/remote_actor.hpp>
#include <unordered_set>
#include <sstream>

#ifdef DEBUG
// So that we don't have a recursive expansion from sending messages via the
// report::manager endpoint.
#define BROKER_ENDPOINT_DEBUG(endpoint_pointer, subtopic, msg) \
	if ( endpoint_pointer != broker::report::manager ) \
		broker::report::send(broker::report::level::debug, subtopic, msg)
#else
#define BROKER_ENDPOINT_DEBUG(endpoint_pointer, subtopic, msg)
#endif

namespace broker {

static std::string to_string(const topic_set& ts)
	{
	std::string rval{"{"};

	bool first = true;

	for ( const auto& e : ts )
		{
		if ( first )
			first = false;
		else
			rval += ", ";

		rval += e.first;
		}

	rval += "}";
	return rval;
	}

static void ocs_update(const caf::actor& q, peering::impl pi,
                       outgoing_connection_status::tag t, std::string name = "")
	{
	peering p{std::unique_ptr<peering::impl>(new peering::impl(std::move(pi)))};
	caf::anon_send(q, outgoing_connection_status{std::move(p), t,
	                                             std::move(name)});
	}

static void ics_update(const caf::actor& q, std::string name,
                       incoming_connection_status::tag t)
	{ caf::anon_send(q, incoming_connection_status{t, std::move(name)}); }

class endpoint_actor : public caf::event_based_actor {

public:

	endpoint_actor(const endpoint* ep, std::string arg_name, int flags,
	               caf::actor ocs_queue, caf::actor ics_queue)
		: name(std::move(arg_name)), behavior_flags(flags)
		{
		using namespace caf;
		using namespace std;
		auto ocs_established = outgoing_connection_status::tag::established;
		auto ocs_disconnect = outgoing_connection_status::tag::disconnected;
		auto ocs_incompat = outgoing_connection_status::tag::incompatible;

		active = {
		[=](int version)
			{
			return make_message(BROKER_PROTOCOL_VERSION == version,
			                    BROKER_PROTOCOL_VERSION);
			},
		[=](peer_atom, actor& p, peering::impl& pi)
			{
			auto it = peers.find(p.address());

			if ( it != peers.end() )
				{
				ocs_update(ocs_queue, move(pi), ocs_established,
		                   it->second.name);
				return;
				}

			sync_send(p, BROKER_PROTOCOL_VERSION).then(
				[=](const sync_exited_msg& m)
					{
					ocs_update(ocs_queue, move(pi), ocs_disconnect);
					},
				[=](bool compat, int their_version)
					{
					if ( ! compat )
						ocs_update(ocs_queue, move(pi), ocs_incompat);
					else
						sync_send(p, peer_atom::value, this, name,
						          advertised_subscriptions).then(
							[=](const sync_exited_msg& m)
								{
								ocs_update(ocs_queue, move(pi), ocs_disconnect);
								},
							[=](string& pname, topic_set& ts)
								{
								add_peer(move(p), pname, move(ts), false);
								ocs_update(ocs_queue, move(pi), ocs_established,
								           move(pname));
								}
						);
					},
				others() >> [=]
					{
					ocs_update(ocs_queue, move(pi), ocs_incompat);
					}
			);
			},
		[=](peer_atom, actor& p, string& pname, topic_set& ts)
			{
			ics_update(ics_queue, pname,
			           incoming_connection_status::tag::established);
			add_peer(move(p), move(pname), move(ts), true);
			return make_message(name, advertised_subscriptions);
			},
		[=](unpeer_atom, const actor& p)
			{
			auto itp = peers.find(p.address());

			if ( itp == peers.end() )
			    return;

			BROKER_DEBUG("endpoint." + name,
			             "Unpeered with: '" + itp->second.name + "'");

			if ( itp->second.incoming )
				ics_update(ics_queue, itp->second.name,
				           incoming_connection_status::tag::disconnected);

			demonitor(p);
			peers.erase(itp);
			peer_subscriptions.erase(p.address());
			},
		[=](const down_msg& d)
			{
			demonitor(d.source);

			auto itp = peers.find(d.source);

			if ( itp != peers.end() )
				{
				BROKER_DEBUG("endpoint." + name,
				             "Peer down: '" + itp->second.name + "'");

				if ( itp->second.incoming )
					ics_update(ics_queue, itp->second.name,
					           incoming_connection_status::tag::disconnected);

				peers.erase(itp);
				peer_subscriptions.erase(d.source);
				return;
				}

			auto s = local_subscriptions.erase(d.source);

			if ( ! s )
				return;

			BROKER_DEBUG("endpoint." + name,
			             "Local subscriber down with subscriptions: "
			             + to_string(s->subscriptions));

			for ( auto& sub : s->subscriptions )
				if ( ! local_subscriptions.have_subscriber_for(sub.first) )
					unadvertise_subscription(topic{move(sub.first)});
			},
		[=](unsub_atom, const topic& t, const actor& p)
			{
			BROKER_DEBUG("endpoint." + name,
			             "Peer '" + get_peer_name(p) + "' unsubscribed to '"
			             + t + "'");
			peer_subscriptions.unregister_topic(t, p.address());
			},
		[=](sub_atom, topic& t, actor& p)
			{
			BROKER_DEBUG("endpoint." + name,
			             "Peer '" + get_peer_name(p) + "' subscribed to '"
			             + t + "'");
			peer_subscriptions.register_topic(move(t), move(p));
			},
		[=](master_atom, store::identifier& id, actor& a)
			{
			if ( local_subscriptions.exact_match(id) )
				{
				report::error("endpoint." + name + ".store.master." + id,
				              "Failed to register master data store with id '"
				              + id + "' because a master already exists with"
				                     " that id.");
				return;
				}

			BROKER_DEBUG("endpoint." + name,
			             "Attached master data store named '" + id + "'");
			attach(move(id), move(a));
			},
		[=](local_sub_atom, topic& t, actor& a)
			{
			BROKER_DEBUG("endpoint." + name,
			             "Attached local queue for topic '" + t + "'");
			attach(move(t), move(a));
			},
		[=](const topic& t, broker::message& msg, int flags)
			{
			bool from_peer = peers.find(current_sender()) != peers.end();

			if ( from_peer )
				{
				BROKER_ENDPOINT_DEBUG(ep, "endpoint." + name,
				                      "Got remote message from peer '"
				                      + get_peer_name(current_sender())
				                      + "', topic '" + t + "': "
				                      + to_string(msg));
				publish_locally(t, std::move(msg), flags, from_peer);
				// Don't re-publish messages sent by a peer (they go one hop).
				}
			else
				{
				BROKER_ENDPOINT_DEBUG(ep, "endpoint." + name,
				                      "Publish local message with topic '" + t
				                      + "': " + to_string(msg));
				publish_locally(t, msg, flags, from_peer);
				publish_current_msg_to_peers(t, flags);
				}
			},
		[=](store_actor_atom, const store::identifier& n)
			{
			return find_master(n);
			},
		[=](const store::identifier& n, const store::query& q,
		    const actor& requester)
			{
			auto master = find_master(n);

			if ( master )
				{
				BROKER_DEBUG("endpoint." + name, "Forwarded data store query: "
				             + caf::to_string(current_message()));
				forward_to(master);
				}
			else
				{
				BROKER_DEBUG("endpoint." + name,
				             "Failed to forward data store query: "
				             + caf::to_string(current_message()));
				send(requester, this,
				     store::result(store::result::status::failure));
				}
			},
		on<store::identifier, anything>() >> [=](const store::identifier& id)
			{
			// This message should be a store update operation.
			auto master = find_master(id);

			if ( master )
				{
				BROKER_DEBUG("endpoint." + name, "Forwarded data store update: "
				             + caf::to_string(current_message()));
				forward_to(master);
				}
			else
				report::warn("endpoint." + name + ".store.master." + id,
				             "Data store update dropped due to nonexistent "
				             " master with id '" + id + "'");
			},
		[=](flags_atom, int flags)
			{
			bool auto_before = (behavior_flags & AUTO_ADVERTISE);
			behavior_flags = flags;
			bool auto_after = (behavior_flags & AUTO_ADVERTISE);

			if ( auto_before == auto_after )
				return;

			if ( auto_before )
				{
				topic_set to_remove;

				for ( const auto& t : advertised_subscriptions )
					if ( advert_acls.find(t.first) == advert_acls.end() )
						to_remove.insert({t.first, true});

				BROKER_DEBUG("endpoint." + name, "Toggled AUTO_ADVERTISE off,"
				                                 " no longer advertising: "
				             + to_string(to_remove));

				for ( const auto& t : to_remove )
					unadvertise_subscription(topic{t.first});

				return;
				}

			BROKER_DEBUG("endpoint." + name, "Toggled AUTO_ADVERTISE on");

			for ( const auto& t : local_subscriptions.topics() )
				advertise_subscription(topic{t.first});
			},
		[=](acl_pub_atom, topic& t)
			{
			BROKER_DEBUG("endpoint." + name, "Allow publishing topic: " + t);
			pub_acls.insert({move(t), true});
			},
		[=](acl_unpub_atom, const topic& t)
			{
			BROKER_DEBUG("endpoint." + name, "Disallow publishing topic: " + t);
			pub_acls.erase(t);
			},
		[=](advert_atom, string& t)
			{
			BROKER_DEBUG("endpoint." + name,
			             "Allow advertising subscription: " + t);

			if ( advert_acls.insert({t, true}).second &&
			     local_subscriptions.exact_match(t) )
				// Now permitted to advertise an existing subscription.
				advertise_subscription(move(t));
			},
		[=](unadvert_atom, string& t)
			{
			BROKER_DEBUG("endpoint." + name,
			             "Disallow advertising subscription: " + t);

			if ( advert_acls.erase(t) && local_subscriptions.exact_match(t) )
				// No longer permitted to advertise an existing subscription.
				unadvertise_subscription(move(t));
			},
		others() >> [=]
			{
			report::warn("endpoint." + name, "Got unexpected message: "
			             + caf::to_string(current_message()));
			}
		};
		}

private:

	caf::behavior make_behavior() override
		{
		return active;
		}

	std::string get_peer_name(const caf::actor_addr& a) const
		{
		auto it = peers.find(a);

		if ( it == peers.end() )
			return "<unknown>";

		return it->second.name;
		}

	std::string get_peer_name(const caf::actor& p) const
		{ return get_peer_name(p.address()); }

	void add_peer(caf::actor p, std::string peer_name, topic_set ts,
	              bool incoming)
		{
		BROKER_DEBUG("endpoint." + name, "Peered with: '" + peer_name
		             + "', subscriptions: " + to_string(ts));
		demonitor(p);
		monitor(p);
		peers[p.address()] = {p, std::move(peer_name), incoming};
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
			{
			BROKER_DEBUG("endpoint." + name,"Advertise new subscription: " + t);
			publish_subscription_operation(std::move(t), sub_atom::value);
			}
		}

	void unadvertise_subscription(topic t)
		{
		if ( advertised_subscriptions.erase(t) )
			{
			BROKER_DEBUG("endpoint." + name, "Unadvertise subscription: " + t);
			publish_subscription_operation(std::move(t), unsub_atom::value);
			}
		}

	void publish_subscription_operation(topic t, caf::atom_value op)
		{
		if ( peers.empty() )
			return;

		auto msg = caf::make_message(std::move(op), std::move(t), this);

		for ( const auto& p : peers )
			send(p.second.ep, msg);
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
				send(a, caf_msg);
		}

	void publish_current_msg_to_peers(const topic& t, int flags)
		{
		if ( ! (flags & PEERS) )
			return;

		if ( ! (behavior_flags & AUTO_PUBLISH) &&
		     pub_acls.find(t) == pub_acls.end() )
			// Not allowed to publish this topic to peers.
			return;

        // send instead of forward_to so peer can use
        // current_sender() to check if msg comes from a peer.
        if ( (flags & UNSOLICITED) )
            for ( const auto& p : peers )
                send(p.second.ep, current_message());
        else
            for ( const auto& a : peer_subscriptions.unique_prefix_matches(t) )
                send(a, current_message());
		}

	struct peer_endpoint {
		caf::actor ep;
		std::string name;
		bool incoming;
	};

	caf::behavior active;

	std::string name;
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
class endpoint_proxy_actor : public caf::event_based_actor {

public:

	endpoint_proxy_actor(caf::actor local, std::string endpoint_name,
	                     std::string addr, uint16_t port,
	                     std::chrono::duration<double> retry_freq,
	                     caf::actor ocs_queue)
		{
		using namespace caf;
		using namespace std;
		peering::impl pi(local, this, true, make_pair(addr, port));

		trap_exit(true);

		bootstrap = {
		after(chrono::seconds(0)) >> [=]
			{
			try_connect(pi, endpoint_name);
			}
		};

		disconnected = {
		[=](peerstat_atom)
			{
			ocs_update(ocs_queue, pi,
		               outgoing_connection_status::tag::disconnected);
			},
		[=](const exit_msg& e)
			{
			quit();
			},
		[=](quit_atom)
			{
			quit();
			},
		after(chrono::duration_cast<chrono::microseconds>(retry_freq)) >> [=]
			{
			try_connect(pi, endpoint_name);
			}
		};

		connected = {
		[=](peerstat_atom)
			{
			send(local, peer_atom::value, remote, pi);
			},
		[=](const exit_msg& e)
			{
			send(remote, unpeer_atom::value, local);
			send(local, unpeer_atom::value, remote);
			quit();
			},
		[=](quit_atom)
			{
			send(remote, unpeer_atom::value, local);
			send(local, unpeer_atom::value, remote);
			quit();
			},
		[=](const down_msg& d)
			{
			BROKER_DEBUG(report_subtopic(endpoint_name, addr, port),
			             "Disconnected from peer");
			demonitor(remote);
			remote = invalid_actor;
			become(disconnected);
			ocs_update(ocs_queue, pi,
		               outgoing_connection_status::tag::disconnected);
			},
		others() >> [=]
			{
			report::warn(report_subtopic(endpoint_name, addr, port),
			             "Remote endpoint proxy got unexpected message: "
			             + caf::to_string(current_message()));
			}
		};
		}

private:

	caf::behavior make_behavior() override
		{
		return bootstrap;
		}

	std::string report_subtopic(const std::string& endpoint_name,
	                            const std::string& addr, uint16_t port) const
		{
		std::ostringstream st;
		st << "endpoint." << endpoint_name << ".remote_proxy." << addr
		   << ":" << port;
		return st.str();
		}

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
			report::warn(report_subtopic(endpoint_name, addr, port),
			             string("Failed to connect: ") + e.what());
			}

		if ( ! remote )
			{
			become(disconnected);
			return false;
			}

		BROKER_DEBUG(report_subtopic(endpoint_name, addr, port), "Connected");
		monitor(remote);
		become(connected);
		send(local, peer_atom::value, remote, pi);
		return true;
		}

	caf::actor remote = caf::invalid_actor;
	caf::behavior bootstrap;
	caf::behavior disconnected;
	caf::behavior connected;
};

static inline caf::actor& handle_to_actor(void* h)
	{ return *static_cast<caf::actor*>(h); }

class endpoint::impl {
public:

	impl(const endpoint* ep, std::string n, int arg_flags)
		: name(std::move(n)), flags(arg_flags), self(),
		  outgoing_conns(), incoming_conns(),
		  actor(caf::spawn<broker::endpoint_actor>(ep, name, flags,
		                   handle_to_actor(outgoing_conns.handle()),
		                   handle_to_actor(incoming_conns.handle()))),
		  peers(), last_errno(), last_error()
		{
		self->planned_exit_reason(caf::exit_reason::user_defined);
		actor->link_to(self);
		}

	std::string name;
	int flags;
	caf::scoped_actor self;
	outgoing_connection_status_queue outgoing_conns;
	incoming_connection_status_queue incoming_conns;
	caf::actor actor;
	std::unordered_set<peering> peers;
	int last_errno;
	std::string last_error;
};
} // namespace broker

#endif // BROKER_ENDPOINT_IMPL_HH
