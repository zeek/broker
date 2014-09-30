#ifndef BROKER_ENDPOINT_IMPL_HH
#define BROKER_ENDPOINT_IMPL_HH

#include "broker/endpoint.hh"
#include "broker/peer_status.hh"
#include "broker/store/store.hh"
#include "broker/store/query.hh"
#include "subscription.hh"
#include "peering_impl.hh"
#include <caf/actor.hpp>
#include <caf/spawn.hpp>
#include <caf/send.hpp>
#include <caf/sb_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/io/remote_actor.hpp>
#include <unordered_set>
#include <string>

namespace broker {

static void do_peer_status(const caf::actor q, peering::impl pi,
                           peer_status::type t)
	{
	peering p{std::unique_ptr<peering::impl>(new peering::impl(std::move(pi)))};
	caf::anon_send(q, peer_status{std::move(p), t});
	}

class endpoint_actor : public caf::sb_actor<endpoint_actor> {
friend class caf::sb_actor<endpoint_actor>;

public:

	endpoint_actor(caf::actor peer_status_q)
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
			if ( peers.find(p.address()) != peers.end() )
				{
				do_peer_status(peer_status_q, move(pi),
				               peer_status::type::established);
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
						sync_send(p, atom("peer"), this,
						          local_subs.topics()).then(
							on_arg_match >> [=](const sync_exited_msg& m)
								{
								do_peer_status(peer_status_q, move(pi),
								               peer_status::type::disconnected);
								},
							on_arg_match >> [=](subscriptions& topics)
								{
								add_peer(move(p), move(topics));
								do_peer_status(peer_status_q, move(pi),
								               peer_status::type::established);
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
		on(atom("peer"), arg_match) >> [=](actor& p, subscriptions& t)
			{
			add_peer(move(p), move(t));
			return make_message(local_subs.topics());
			},
		on(atom("unpeer"), arg_match) >> [=](const actor& p)
			{
			demonitor(p);
			peers.erase(p.address());
			peer_subs.rem_subscriber(p.address());
			},
		on_arg_match >> [=](const down_msg& d)
			{
			demonitor(d.source);
			peers.erase(d.source);
			peer_subs.rem_subscriber(d.source);
			subscriptions unsubs = local_subs.rem_subscriber(d.source);

			for ( const auto& p : peers )
				send(p.second, atom("unsub"), unsubs, this);
			},
		on(atom("unsub"), arg_match) >> [=](const subscriptions& topics,
		                                    const actor& p)
			{
			peer_subs.rem_subscriptions(topics, p.address());
			},
		on(atom("sub"), arg_match) >> [=](subscription& t, actor& a)
			{
			demonitor(a);
			monitor(a);
			local_subs.add_subscription(t, move(a));

			for ( const auto& p : peers )
				send(p.second, atom("subpeer"), t, this);
			},
		on(atom("subpeer"), arg_match) >> [=](subscription& t, actor& p)
			{
			peer_subs.add_subscription(move(t), move(p));
			},
		on_arg_match >> [=](const subscription& s, const query& q,
		                    const actor& requester)
			{
			auto master = find_master(s);

			if ( ! master )
				send(requester, this, result(result::status::failure));
			else
				forward_to(master);
			},
		on<subscription, anything>() >> [=](const subscription& t)
			{
			publish_current_msg(t);
			}
		);
		}

private:

	void add_peer(caf::actor p, subscriptions t)
		{
		demonitor(p);
		monitor(p);
		peers[p.address()] = p;
		peer_subs.add_subscriber(subscriber{std::move(t), p});
		}

	caf::actor find_master(const subscription& t)
		{
		auto m = local_subs.match(t);

		if ( m.empty() )
			m = peer_subs.match(t);

		if ( m.empty() )
			return caf::invalid_actor;

		return *m.begin();
		}

	void publish_current_msg(const subscription& topic)
		{
		for ( const auto& a : local_subs.match(topic) )
			forward_to(a);

		if ( peers.find(last_sender()) != peers.end() )
			// Don't re-publish messages published by a peer (they go one hop).
			return;

		for ( const auto& a : peer_subs.match(topic) )
			// Use send_tuple instead of forward_to for above re-publish check.
			send_tuple(a, last_dequeued());
		}

	caf::behavior active;
	caf::behavior& init_state = active;

	actor_map peers;
	subscriber_base local_subs;
	subscriber_base peer_subs;
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

	impl(std::string n)
		: name(std::move(n)), self(), peer_status(),
		  actor(caf::spawn<broker::endpoint_actor>(
		                   handle_to_actor(peer_status.handle()))),
		  peers(), last_errno(), last_error()
		{
		self->planned_exit_reason(caf::exit_reason::user_defined);
		actor->link_to(self);
		}

	std::string name;
	caf::scoped_actor self;
	peer_status_queue peer_status;
	caf::actor actor;
	std::unordered_set<peering> peers;
	int last_errno;
	std::string last_error;
};
} // namespace broker

#endif // BROKER_ENDPOINT_IMPL_HH
