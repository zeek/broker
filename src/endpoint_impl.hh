#ifndef BROKER_ENDPOINT_IMPL_HH
#define BROKER_ENDPOINT_IMPL_HH

#include "broker/endpoint.hh"
#include "subscription.hh"
#include "peering_impl.hh"
#include "broker/store/store.hh"
#include "broker/store/query.hh"
#include <caf/actor.hpp>
#include <caf/spawn.hpp>
#include <caf/send.hpp>
#include <caf/sb_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/io/remote_actor.hpp>
#include <unordered_set>
#include <string>

namespace broker {

struct incompatible_endpoint {
	caf::actor actor;
	int version;
};

class endpoint_actor : public caf::sb_actor<endpoint_actor> {
friend class caf::sb_actor<endpoint_actor>;

public:

	endpoint_actor()
		{
		using namespace caf;
		using namespace std;
		using namespace broker::store;

		active = (
		on(atom("peer"), arg_match) >> [=](actor& p, int protocol_version)
			{
			if ( protocol_version != BROKER_PROTOCOL_VERSION )
				{
				send(p, atom("peer"), this, BROKER_PROTOCOL_VERSION);
				add_incompatible_peer(move(p), protocol_version);
				return;
				}

			sync_send(p, atom("peer"), this, local_subs.topics()).then(
				on_arg_match >> [=](const sync_exited_msg& m)
					{
					},
				on_arg_match >> [=](subscriptions& topics)
					{
					add_peer(move(p), move(topics));
					}
			);
			},
		on(atom("peer"), arg_match) >> [=](actor& p, subscriptions& t)
			{
			add_peer(move(p), move(t));
			return make_message(local_subs.topics());
			},
		on(atom("handshake"), arg_match) >> [=](actor& p, actor& observer)
			{
			if ( peers.find(p.address()) != peers.end() )
				{
				send(observer, true, BROKER_PROTOCOL_VERSION);
				return;
				}

			if ( incompatible.find(p.address()) != incompatible.end() )
				{
				send(observer, false, incompatible[p.address()].version);
				return;
				}

			demonitor(observer);
			monitor(observer);
			hs_observers[observer.address()] = observer;
			hs_pending[move(p)].insert(move(observer));
			},
		on(atom("unpeer"), arg_match) >> [=](const actor& p)
			{
			demonitor(p);

			if ( incompatible.erase(p.address()) )
				return;

			peers.erase(p.address());
			peer_subs.rem_subscriber(p.address());
			},
		on_arg_match >> [=](const down_msg& d)
			{
			demonitor(d.source);

			if ( remove_observer(d.source) )
				return;

			if ( incompatible.erase(d.source) )
				return;

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

	void add_incompatible_peer(caf::actor p, int protocol_version)
		{
		demonitor(p);
		monitor(p);
		incompatible[p.address()] = incompatible_endpoint{p, protocol_version};
		auto msg =caf::make_message(false, protocol_version);
		check_pending_handshakes(std::move(p), std::move(msg));
		}

	void add_peer(caf::actor p, subscriptions t)
		{
		demonitor(p);
		monitor(p);
		peers[p.address()] = p;
		peer_subs.add_subscriber(subscriber{std::move(t), p});
		auto msg = caf::make_message(true, BROKER_PROTOCOL_VERSION);
		check_pending_handshakes(std::move(p), std::move(msg));
		}

	bool remove_observer(const caf::actor_addr& o)
		{
		auto it = hs_observers.find(o);

		if ( it == hs_observers.end() )
			return false;

		for ( auto& p : hs_pending )
			if ( p.second.erase(it->second) )
				{
				if ( p.second.empty() )
					hs_pending.erase(p.first);

				break;
				}

		hs_observers.erase(it);
		return true;
		}

	void check_pending_handshakes(caf::actor p, caf::message msg)
		{
		auto it = hs_pending.find(p);

		if ( it != hs_pending.end() )
			{
			for ( auto& o : it->second )
				{
				send_tuple(o, msg);
				demonitor(o);
				hs_observers.erase(o.address());
				}

			hs_pending.erase(it);
			}
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
	std::unordered_map<caf::actor_addr, incompatible_endpoint> incompatible;
	actor_map hs_observers;
	std::unordered_map<caf::actor, std::unordered_set<caf::actor>> hs_pending;
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
	                     std::chrono::duration<double> retry_freq)
		{
		using namespace caf;
		using namespace std;

		trap_exit(true);

		bootstrap = (
		after(chrono::seconds(0)) >> [=]
			{
			try_connect(addr, port, local);
			}
		);

		disconnected = (
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
			try_connect(addr, port, local);
			}
		);

		connected = (
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
		on(atom("handshake"), any_vals) >> [=]
			{
			forward_to(remote);
			},
		on_arg_match >> [=](const down_msg& d)
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
		send(remote, atom("peer"), local, BROKER_PROTOCOL_VERSION);
		return true;
		}

	caf::actor remote = caf::invalid_actor;
	caf::behavior bootstrap;
	caf::behavior disconnected;
	caf::behavior connected;
	caf::behavior& init_state = bootstrap;
};

class endpoint::impl {
public:

	impl(std::string n)
		: name(std::move(n)), actor(caf::spawn<broker::endpoint_actor>())
		{
		self->planned_exit_reason(caf::exit_reason::user_defined);
		actor->link_to(self);
		}

	std::string name;
	caf::scoped_actor self;
	caf::actor actor;
	std::unordered_set<peering> peers;
	int last_errno;
	std::string last_error;
};
} // namespace broker

#endif // BROKER_ENDPOINT_IMPL_HH
