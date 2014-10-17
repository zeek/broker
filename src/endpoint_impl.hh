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
                           peer_status::type t, std::string pname = "")
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
				               peer_status::type::established, it->second.name);
				return;
				}

			sync_send(p, BROKER_PROTOCOL_VERSION).then(
				on_arg_match >> [=](const sync_exited_msg& m)
					{
					do_peer_status(peer_status_q, move(pi),
					               peer_status::type::disconnected);
					},
			    on_arg_match >> [=](bool compat, int their_version)
					{
					if ( ! compat )
						do_peer_status(peer_status_q, move(pi),
						               peer_status::type::incompatible);
					else
						sync_send(p, atom("peer"), this, name,
						          local_subscriptions.topics()).then(
							on_arg_match >> [=](const sync_exited_msg& m)
								{
								do_peer_status(peer_status_q, move(pi),
								               peer_status::type::disconnected);
								},
							on_arg_match >> [=](std::string& pname,
							                    topic_set& topics)
								{
								add_peer(move(p), pname, move(topics));
								do_peer_status(peer_status_q, move(pi),
								               peer_status::type::established,
								               move(pname));
								}
						);
					},
				others() >> [=]
					{
					do_peer_status(peer_status_q, move(pi),
					               peer_status::type::incompatible);
					}
			);
			},
		on(atom("peer"), arg_match) >> [=](actor& p, std::string& pname,
		                                   topic_set& t)
			{
			add_peer(move(p), move(pname), move(t));
			return make_message(name, local_subscriptions.topics());
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

			for ( const auto& p : peers )
				send(p.second.ep,
				     atom("unsub"), std::move(s->subscriptions), this);
			},
		on(atom("unsub"), arg_match) >> [=](const topic_set& topics,
		                                    const actor& p)
			{
			peer_subscriptions.unregister_topics(topics, p.address());
			},
		on(atom("sub"), arg_match) >> [=](topic& t, actor& a)
			{
			demonitor(a);
			monitor(a);
			local_subscriptions.register_topic(t, move(a));

			for ( const auto& p : peers )
				send(p.second.ep, atom("subpeer"), t, this);
			},
		on(atom("subpeer"), arg_match) >> [=](topic& t, actor& p)
			{
			peer_subscriptions.register_topic(move(t), move(p));
			},
		on_arg_match >> [=](const topic& s, const query& q,
		                    const actor& requester)
			{
			auto master = find_master(s);

			if ( ! master )
				send(requester, this, result(result::status::failure));
			else
				forward_to(master);
			},
		on<topic, anything>() >> [=](const topic& t)
			{
			publish_current_msg(t);
			},
		on(atom("flags"), arg_match) >> [=](int flags)
			{
			behavior_flags = flags;
			},
		on(caf::atom("acl pub"), arg_match) >>[=](topic& t)
			{
			pub_acls[+t.type].insert(make_pair(std::move(t.name), true));
			},
		on(caf::atom("acl unpub"), arg_match) >>[=](const topic& t)
			{
			pub_acls[+t.type].erase(t.name);
			},
		on(caf::atom("acl sub"), arg_match) >>[=](topic& t)
			{
			sub_acls[+t.type].insert(make_pair(std::move(t.name), true));
			},
		on(caf::atom("acl unsub"), arg_match) >>[=](const topic& t)
			{
			sub_acls[+t.type].erase(t.name);
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
		auto m = local_subscriptions.match_exact(t);

		if ( ! m )
			m = peer_subscriptions.match_exact(t);

		if ( ! m )
			return caf::invalid_actor;

		return *m->begin();
		}

	void publish_current_msg(const topic& t)
		{
		for ( const auto& match : local_subscriptions.match_prefix(t) )
			for ( const auto& a : match->second )
				forward_to(a);

		if ( peers.find(last_sender()) != peers.end() )
			// Don't re-publish messages published by a peer (they go one hop).
			return;

		for ( const auto& match : peer_subscriptions.match_prefix(t) )
			for ( const auto& a : match->second )
				// send_tuple instead of forward_to for above re-publish check.
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
			do_peer_status(peer_status_q, pi, peer_status::type::disconnected);
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
			do_peer_status(peer_status_q, pi, peer_status::type::disconnected);
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
