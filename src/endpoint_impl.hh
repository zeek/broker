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
#include <assert.h>
#include <exception>


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

			BROKER_DEBUG(name, " protocol version is " + to_string(BROKER_PROTOCOL_VERSION));
			sync_send(p, BROKER_PROTOCOL_VERSION).then(
				[=](const sync_exited_msg& m)
					{
					BROKER_DEBUG(name, "received sync exit (1)");
					ocs_update(ocs_queue, move(pi), ocs_disconnect);
					},
				[=](bool compat, int their_version)
					{
					if ( ! compat )
						{
						BROKER_DEBUG(name, " broker version not compatible with peer");
						ocs_update(ocs_queue, move(pi), ocs_incompat);
						}
					else
						{
						BROKER_DEBUG(name, " initiate peering with " + get_peer_name(p));

						sync_send(p, peer_atom::value, this, name,
						          advertised_subscriptions_single, 
											get_all_subscriptions()).then(
							[=](const sync_exited_msg& m)
								{
								BROKER_DEBUG(name, "received sync exit (2)");
								ocs_update(ocs_queue, move(pi), ocs_disconnect);
								},
							[=](string& pname, topic_set& ts_single, topic_set& ts_multi)
								{
								BROKER_DEBUG(name, "received peer_atom response by sender " 
															+ caf::to_string(p) + " - " + pname 
															+ " with topics single" + to_string(ts_single)
															+ " and multi" + to_string(ts_multi));

								add_peer(move(p), pname, false, move(ts_single), move(ts_multi));
								ocs_update(ocs_queue, move(pi), ocs_established, move(pname));
								}
						);
						}
					},
				others() >> [=]
					{
					ocs_update(ocs_queue, move(pi), ocs_incompat);
					}
			);
			},
		[=](peer_atom, actor& p, string& pname, topic_set& ts_single, topic_set& ts_multi)
			{
			BROKER_DEBUG(name, " received peer_atom from sender "
										+ caf::to_string(p) + " - " + pname
										+ " with topics single" + to_string(ts_single)
										+ " and multi " + to_string(ts_multi));

			ics_update(ics_queue, pname,
			           incoming_connection_status::tag::established);

			add_peer(move(p), move(pname), true, move(ts_single), move(ts_multi));

			// send back the message
			return make_message(name, advertised_subscriptions_single, get_all_subscriptions());
			},
		[=](unpeer_atom, const actor& p)
			{
			auto itp = peers.find(p.address());

			if ( itp == peers.end() )
			    return;

			BROKER_DEBUG(name,
			             "Unpeered with: '" + itp->second.name + "'"
									 + caf::to_string(itp->second.ep.address()));

			if ( itp->second.incoming )
				ics_update(ics_queue, itp->second.name,
				           incoming_connection_status::tag::disconnected);

			// unregister the peer
			remove_peer(p);
			},
		[=](const down_msg& d)
			{
			demonitor(d.source);

		  BROKER_DEBUG(name, " (4) own subscriptions are " 
										+ to_string(local_subscriptions_single.topics()) 
										+ " d.source " + get_peer_name(d.source));

			auto itp = peers.find(d.source);
			if ( itp != peers.end() )
				{
				BROKER_DEBUG(name, "Peer down: '" + itp->second.name + "'");

				if ( itp->second.incoming )
					ics_update(ics_queue, itp->second.name,
					           incoming_connection_status::tag::disconnected);

				assert(d.source == itp->second.ep.address());

				// unregister the peer
				remove_peer(itp->second.ep);
				return;
				}

			auto s = local_subscriptions_single.erase(d.source);
		  BROKER_DEBUG(name, " (5) own subscriptions are " + to_string(local_subscriptions_single.topics()));

			if ( ! s )
				return;

			BROKER_DEBUG(name,
			             "Local subscriber down with subscriptions: "
			             + to_string(s->subscriptions));

			for ( auto& sub : s->subscriptions )
				if ( ! local_subscriptions_single.have_subscriber_for(sub.first) )
					unadvertise_subscription(topic{move(sub.first)});
			},
		[=](unsub_atom, const topic& t, const actor& p)
			{
			BROKER_DEBUG(name,
									 "Unsubscription received for topic '" 
									 + t + " from " + get_peer_name(p) 
									 + " - " + caf::to_string(p.address()));

				unregister_subscription(t, p);
			},
		[=](munsub_atom, const topic& t, const actor& p)
			{
			BROKER_DEBUG(name,
									 "Unsubscription received for multi-hop topic '" 
									 + t + " from " + get_peer_name(p) 
									 + " - " + caf::to_string(p.address()));

				unregister_subscription(t, p, true);
			},
		[=](sub_atom, topic& t, actor& p)
			{
			BROKER_DEBUG(name, 
									 "Single-hop subscription received: Peer '" + get_peer_name(p) 
									 + "' subscribed to '" + t);
			register_subscription(t, p);
			},
		[=](msub_atom, topic& t, actor& p)
			{
			BROKER_DEBUG(name, 
									 "Multi-hop subscription received: Peer '" + get_peer_name(p) 
									 + "' subscribed to '" + t);
			register_subscription(t, p, true);
			},
		[=](master_atom, store::identifier& id, actor& a)
			{
			if ( local_subscriptions_single.exact_match(id) )
				{
				report::error(name + ".store.master." + id,
				              "Failed to register master data store with id '"
				              + id + "' because a master already exists with"
				                     " that id.");
				return;
				}

			BROKER_DEBUG(name,
			             "Attached master data store named '" + id + "'");
			attach(move(id), move(a));
			},
		[=](local_sub_atom, topic& t, actor& a)
			{
			BROKER_DEBUG(name,
			             caf::to_string(this->address()) 
									 + " attached local queue for topic '" + t + "'");
			attach(move(t), move(a));
			},
		[=](local_msub_atom, topic& t, actor& a)
			{
			BROKER_DEBUG(name,
			             caf::to_string(this->address()) 
									 + " attached local queue for new multi-hop topic '" + t + "'");
			attach(move(t), move(a), true);
			},
		[=](const topic& t, broker::message& msg, int flags)
			{
			// reporting node gives all debugging output
			if(t.find("broker.report.") != std::string::npos )
				{
				msg.pop_back();
				std::cout << t << ": " << to_string(msg) << std::endl;
				return;
				}

			// get ttl value and remove it from message
			int ttl = std::stoi(to_string(msg.back()));
			msg.pop_back();

			// we are the initial sender
			if(!current_sender())
				{
				BROKER_DEBUG(name,
				             "Publish local message with topic '" 
										 + t + "': " + to_string(msg));
				publish_locally(t, msg, flags, false);
				}
			// we received the message from a neighbor
			else
				{
				// decrement ttl
				ttl--;
				BROKER_DEBUG(name,
										 "Got remote message from peer '"
										 + get_peer_name(current_sender())
										 + "', topic '" + t + "': "
										 + to_string(msg)
										 + " with ttl " + to_string(ttl));

				if (ttl == 0)
					{
					report::error(name, "endpoint_impl, ttl counter reached 0. The topology contains a loop! msg from "
												+	get_peer_name(current_sender())
										    + "', topic '" + t + "': "
												+ to_string(msg)
												+ " with ttl " + to_string(ttl));
					std::cout << "ERROR loop detected at " << name << " that received a message from peer '" 
										 << get_peer_name(current_sender())
										 << "', topic '" << t << "': "
										 << to_string(msg)
										 << " with ttl " << to_string(ttl)
										 << std::endl;
					}
				assert(ttl != 0);

				publish_locally(t, msg, flags, true);
				}

			// and add new ttl value
			msg.push_back(std::move(ttl));
			publish_current_msg_to_peers(t, flags);
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
				BROKER_DEBUG(name, "Forwarded data store query: "
										 + caf::to_string(current_message()));
				forward_to(master);
				}
			else
				{
				BROKER_DEBUG(name,
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
				BROKER_DEBUG(name, "Forwarded data store update: "
										 + caf::to_string(current_message()));
				forward_to(master);
				}
			else
				{
				report::warn(name + ".store.master." + id,
														"Data store update dropped due to nonexistent "
														" master with id '" + id + "'");
				}
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

				for ( const auto& t : advertised_subscriptions_single )
								if ( advert_acls.find(t.first) == advert_acls.end() )
												to_remove.insert({t.first, true});

				BROKER_DEBUG(name, "Toggled AUTO_ADVERTISE off,"
											" no longer advertising: "
											+ to_string(to_remove));

				for ( const auto& t : to_remove )
								unadvertise_subscription(topic{t.first});

				return;
				}

			BROKER_DEBUG(name, "Toggled AUTO_ADVERTISE on");

			for ( const auto& t : local_subscriptions_single.topics() )
				advertise_subscription(topic{t.first});
			},
		[=](acl_pub_atom, topic& t)
			{
			BROKER_DEBUG(name, "Allow publishing topic: " + t);
			pub_acls.insert({move(t), true});
			},
		[=](acl_unpub_atom, const topic& t)
			{
			BROKER_DEBUG(name, "Disallow publishing topic: " + t);
			pub_acls.erase(t);
			},
		// TODO single and multi-hop subscriptions
		[=](advert_atom, string& t)
			{
			BROKER_DEBUG(name, "Allow advertising subscription: " + t);
			if ( advert_acls.insert({t, true}).second &&
						local_subscriptions_single.exact_match(t) )
				// Now permitted to advertise an existing subscription.
				advertise_subscription(move(t));
			},
		[=](unadvert_atom, string& t)
			{
			BROKER_DEBUG(name, "Disallow advertising subscription: " + t);
			if ( advert_acls.erase(t) && local_subscriptions_single.exact_match(t) )
				// No longer permitted to advertise an existing subscription.
				unadvertise_subscription(move(t));
			},
		others() >> [=]
			{
			report::warn(name, "Got unexpected message: "
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

	void add_peer(caf::actor p, std::string peer_name, bool incoming, 
								topic_set ts_single, topic_set ts_multi)
		{
		BROKER_DEBUG(name, " Peered with: '" + peer_name
									+ "\n" + peer_name + " subscriptions:" 
									+ "single "  + to_string(ts_single)
									+ ", multi "  + to_string(ts_multi)
									+ "\nown subscriptions:  "
									+ "single "  + to_string(local_subscriptions_single.topics())
									+ ", multi "  + to_string(local_subscriptions_multi.topics()));
		demonitor(p);
		monitor(p);
		peers[p.address()] = {p, peer_name, incoming};
		peer_subscriptions_single.insert(subscriber{p, std::move(ts_single)});

		// TODO iterate over the topic knowledge of the new peer
		for(auto& s: ts_multi)
			{
			if(local_subscriptions_multi.unique_prefix_matches(s.first).empty())
				register_subscription(s.first, p, true);
			}

		peer_subscriptions_multi.insert(subscriber{std::move(p), std::move(ts_multi)});
		}

	void remove_peer(const caf::actor& a)
		{
		BROKER_DEBUG(name, " remove peer " + get_peer_name(a));

		auto remove_set = peer_subscriptions_multi.topics_of_actor(a.address());
		peer_subscriptions_multi.erase(a.address());
		demonitor(a);
		peers.erase(a.address());
		peer_subscriptions_single.erase(a.address());

		// check if we still need these subscriptions 
		// or if we can delete and unsubscribe them at our neighbors
		for (const auto& s: remove_set)
			unregister_subscription(s.first, a, true);
		}

	void attach(std::string topic_or_id, caf::actor a)
		{
		attach(topic_or_id, a, false);
		}

	void attach(std::string topic_or_id, caf::actor a, bool multi_hop)
	 	{
		demonitor(a);
		monitor(a);

		if(!multi_hop)
			local_subscriptions_single.register_topic(topic_or_id, std::move(a));
		else
			local_subscriptions_multi.register_topic(topic_or_id, std::move(a));

		if ( (behavior_flags & AUTO_ADVERTISE) ||
					advert_acls.find(topic_or_id) != advert_acls.end() )
			advertise_subscription(std::move(topic_or_id), this, multi_hop);
	 }

	caf::actor find_master(const store::identifier& id)
		{
		auto m = local_subscriptions_single.exact_match(id);

		if ( ! m )
			m = peer_subscriptions_single.exact_match(id);

		if( ! m )
			m = peer_subscriptions_multi.exact_match(id);

		if ( ! m )
			return caf::invalid_actor;

		return *m->begin();
	 }

	void advertise_subscription(topic t)
		{
		advertise_subscription(t, this);
		}

	void advertise_subscription(topic t, caf::actor a)
		{
		advertise_subscription(t, a, false);
		}

	void advertise_subscription(topic t, caf::actor a, bool multi_hop)
	 {
	 if (!multi_hop && advertised_subscriptions_single.insert({t, true}).second)
		{
		BROKER_DEBUG(name,"Advertise new single-hop subscription: " + t);
		publish_subscription_operation(std::move(t), sub_atom::value, a);
		}

	 if (multi_hop && advertised_subscriptions_multi.insert({t, true}).second)
		{
		BROKER_DEBUG(name,"Advertise new multi-hop subscription: " + t);
		publish_subscription_operation(std::move(t), msub_atom::value, a);
		}
	 }

	void unadvertise_subscription(topic t)
	 {
	 unadvertise_subscription(t, this);
	 }

	//FIXME unsub from multi_hop subscriptions
	void unadvertise_subscription(topic t, caf::actor a)
	 {
	 if ( advertised_subscriptions_single.erase(t) )
	  {
		BROKER_DEBUG(name, "Unadvertise subscription: " + t);
		publish_subscription_operation(std::move(t), unsub_atom::value, a);
		}
	 }

	void publish_subscription_operation(topic t, caf::atom_value op)
	{
		publish_subscription_operation(t, op, caf::actor());
	}

	void publish_subscription_operation(topic t, caf::atom_value op, const caf::actor& skip)
		{
		if ( peers.empty() )
			return;

		// Build the msg
		caf::message msg;
		msg	= caf::make_message(std::move(op), t, this);

	 // Send the msg out
	 for ( const auto& p : peers )
		{
		if(p.second.ep == skip)
	  	continue;
		BROKER_DEBUG(name, caf::to_string(op) + ", forward topic " + t 
								+ " to peer " + p.second.name);
		send(p.second.ep, msg);
		}
	}

	void publish_locally(const topic& t, broker::message msg, int flags,
									bool from_peer)
		{
		if ( ! from_peer && ! (flags & SELF) )
			{
			BROKER_DEBUG(name, "publish_locally, return (! from_peer && ! (flags & SELF)) ");
			return;
			}

		auto matches_single = local_subscriptions_single.prefix_matches(t);
		auto matches_multi = local_subscriptions_multi.prefix_matches(t);

		if ( matches_single.empty() && matches_multi.empty() )
			{
			BROKER_DEBUG(name, "publish_locally, return (matches.empty()): single"
												+ to_string(local_subscriptions_single.topics())
												+ ", multi" + to_string(local_subscriptions_multi.topics()));
			return;
			}

		auto caf_msg = caf::make_message(std::move(msg));

		for ( const auto& match : matches_single )
			for ( const auto& a : match->second )
				send(a, caf_msg);

		for ( const auto& match : matches_multi )
			for ( const auto& a : match->second )
				send(a, caf_msg);
		}

	void publish_current_msg_to_peers(const topic& t, int flags)
		{
		if ( ! (flags & PEERS) )
			{
			BROKER_DEBUG(name, "   - return: ! (flags & PEERS)");
			return;
			}

		if ( ! (behavior_flags & AUTO_PUBLISH) &&
						pub_acls.find(t) == pub_acls.end() )
			{
			BROKER_DEBUG(name, "   - return: ! (behavior_flags & AUTO_PUBLISH) && pub_acls.find(t) == pub_acls.end():"
										+ to_string(behavior_flags) + ", " + to_string(AUTO_PUBLISH) + ", "
										+ to_string((behavior_flags & AUTO_PUBLISH))
									  + ", " + to_string((pub_acls.find(t) == pub_acls.end())) + " for topic " + t);
			// Not allowed to publish this topic to peers.
			return;
			}

		// send instead of forward_to so peer can use
		// current_sender() to check if msg comes from a peer.
		if ( (flags & UNSOLICITED) )
			{
			for ( const auto& p : peers )
				{
				if(current_sender() == p.first)
					continue;
				send(p.second.ep, current_message());
				}
			}
		else 
			{

			// msgs for single-hop subscriptions are only forwarded when we are the publisher 
			if (!current_sender())
				{
				// publish msgs for single-hop subscriptions
				for ( const auto& a : peer_subscriptions_single.unique_prefix_matches(t) )
					{
					BROKER_DEBUG( name, " ------------> publish msg for topic " + t + " to " + get_peer_name(a));
					send(a, current_message());
					}
				}

			// publish msgs for multi-hop subscriptions
			for ( const auto& a : peer_subscriptions_multi.unique_prefix_matches(t) )
				{
				if(current_sender() == a)
					continue;
				assert(a != this);
				BROKER_DEBUG( name, " ------------> publish msg for topic " + t + " to " + get_peer_name(a));
				send(a, current_message());
				}
			}
		}

	void register_subscription(const topic& t, caf::actor a)
		{
		register_subscription(t, a, false);
		}

	void register_subscription(const topic& t, caf::actor a, bool multi_hop)
		{
		if(!multi_hop)
			{
			BROKER_DEBUG(name, " add single-hop subscription for topic " 
												+ t + " via " + get_peer_name(a));
			if(peer_subscriptions_single.register_topic(t, a))
				publish_subscription_operation(t, sub_atom::value, a);
			}
		else
			{
			BROKER_DEBUG(name, " add multi-hop subscription for topic " 
												+ t + " via " + get_peer_name(a));
			if(multi_hop && peer_subscriptions_multi.register_topic(t, a))
				{
				BROKER_DEBUG(name, + "multi subscriptions now: "  + to_string(peer_subscriptions_multi.topics()));
				publish_subscription_operation(t, msub_atom::value, a);
				}
			}
		}

	bool unregister_subscription(const topic& t , const caf::actor& p)
		{
		return unregister_subscription(t, p, false);
		}

	bool unregister_subscription(const topic& t , const caf::actor& p, bool multi_hop)
		{
		BROKER_DEBUG(name, " unsub for topic " + t  + " via " + to_string(p.address()));
		if(!multi_hop)
			peer_subscriptions_single.unregister_topic(t, p.address());
		else 
			{
			peer_subscriptions_multi.unregister_topic(t, p.address());
			if (!local_subscriptions_multi.have_subscriber_for(t) && !peer_subscriptions_multi.have_subscriber_for(t))
				publish_subscription_operation(t, munsub_atom::value, p);
			}

		return true;
		}

	topic_set get_all_subscriptions()
		{
		// get subscriptions of all neighbors
		topic_set all = peer_subscriptions_multi.topics();
		// add all own subscriptions
		for(auto& e: advertised_subscriptions_multi)
			all.insert({e.first, true});

		return all;
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

	subscription_registry local_subscriptions_single;
	subscription_registry local_subscriptions_multi;

	subscription_registry peer_subscriptions_single;
	subscription_registry peer_subscriptions_multi;

	topic_set advertised_subscriptions_single;
	topic_set advertised_subscriptions_multi;
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
