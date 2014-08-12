#ifndef BROKER_ENDPOINT_IMPL_HH
#define BROKER_ENDPOINT_IMPL_HH

#include "broker/endpoint.hh"
#include "subscription.hh"
#include "data/query_types.hh"
#include "peering_impl.hh"
#include <caf/actor.hpp>
#include <caf/spawn.hpp>
#include <caf/send.hpp>
#include <caf/sb_actor.hpp>
#include <caf/io/remote_actor.hpp>
#include <unordered_set>
#include <string>

namespace broker {

class endpoint_actor : public caf::sb_actor<endpoint_actor> {
friend class caf::sb_actor<endpoint_actor>;

public:

	endpoint_actor()
		{
		using namespace caf;
		using namespace std;
		using namespace broker::data;

		// TODO: more generic way to intercept requests?
		auto handle_snapshot = [=](const snapshot_request& r) -> optional<bool>
			{
			auto master = find_master(r.st);
			if ( ! master ) return true;
			forward_to(master);
			return {};
			};

		auto handle_lookup = [=](const lookup_request& r) -> optional<bool>
			{
			auto master = find_master(r.st);
			if ( ! master ) return true;
			forward_to(master);
			return {};
			};

		auto handle_haskey = [=](const has_key_request& r) -> optional<bool>
			{
			auto master = find_master(r.st);
			if ( ! master ) return true;
			forward_to(master);
			return {};
			};

		auto handle_keys = [=](const keys_request& r) -> optional<bool>
			{
			auto master = find_master(r.st);
			if ( ! master ) return true;
			forward_to(master);
			return {};
			};

		auto handle_size = [=](const size_request& r) -> optional<bool>
			{
			auto master = find_master(r.st);
			if ( ! master ) return true;
			forward_to(master);
			return {};
			};

		message_handler data_requests {
		on(handle_snapshot) >> [=](bool)
			{
			return make_message(atom("dne"));
			}, on_arg_match >> [](const snapshot_request&) {},
		on(handle_lookup) >> [=](bool)
			{
			return make_message(atom("dne"));
			}, on_arg_match >> [](const lookup_request&) {},
		on(handle_haskey) >> [=](bool)
			{
			return make_message(atom("dne"));
			}, on_arg_match >> [](const has_key_request&) {},
		on(handle_keys) >> [=](bool)
			{
			return make_message(atom("dne"));
			}, on_arg_match >> [](const keys_request&) {},
		on(handle_size) >> [=](bool)
			{
			return make_message(atom("dne"));
			}, on_arg_match >> [](const size_request&) {}
		};

		active = data_requests.or_else(
		on(atom("quit")) >> [=]
			{
			//aout(this) << "quit" << endl;
			quit();
			},
		on(atom("peer"), arg_match) >> [=](actor p)
			{
			//aout(this) << "peer" << endl;
			sync_send(p, atom("peer"), this, local_subs.topics()).then(
				on_arg_match >> [=](sync_exited_msg m) { },
				on_arg_match >> [=](subscriptions topics)
					{
					//aout(this) << "got peer response" << endl;
					demonitor(p);
					monitor(p);
					peers[p.address()] = p;
					peer_subs.add_subscriber(subscriber{move(topics), p});
					check_pending_handshakes(p);
					}
			);
			},
		on(atom("peer"), arg_match) >> [=](actor p, subscriptions t)
			{
			demonitor(p);
			monitor(p);
			peers[p.address()] = p;
			peer_subs.add_subscriber(subscriber{move(t), p});
			check_pending_handshakes(p);
			return make_message(local_subs.topics());
			},
		on(atom("handshake"), arg_match) >> [=](actor p, actor observer)
			{
			if ( peers.find(p.address()) != peers.end() )
				{
				send(observer, atom("done"));
				return;
				}

			demonitor(observer);
			monitor(observer);
			hs_observers[observer.address()] = observer;
			hs_pending[p].insert(observer);
			},
		on(atom("unpeer"), arg_match) >> [=](actor p)
			{
			//aout(this) << "unpeer" << endl;
			demonitor(p);
			peers.erase(p.address());
			peer_subs.rem_subscriber(p.address());

			if ( p.address() != last_sender() )
				send(p, atom("unpeer"), this);
			},
		on_arg_match >> [=](down_msg d)
			{
			//aout(this) << "down" << endl;
			if ( remove_observer(d.source) )
				return;

			demonitor(d.source);
			peers.erase(d.source);
			peer_subs.rem_subscriber(d.source);
			subscriptions unsubs = local_subs.rem_subscriber(d.source);

			for ( const auto& p : peers )
				send(p.second, atom("unsub"), unsubs, this);
			},
		on(atom("unsub"), arg_match) >> [=](subscriptions topics, actor p)
			{
			peer_subs.rem_subscriptions(topics, p.address());
			},
		on(atom("sub"), arg_match) >> [=](subscription t, actor subscriber)
			{
			demonitor(subscriber);
			monitor(subscriber);
			local_subs.add_subscription(t, move(subscriber));

			for ( const auto& p : peers )
				send(p.second, atom("subpeer"), t, this);
			},
		on(atom("subpeer"), arg_match) >> [=](subscription t, actor p)
			{
			peer_subs.add_subscription(move(t), p);
			},
		on<subscription, anything>() >> [=](subscription t)
			{
			//aout(this) << "publish: " << to_string(last_dequeued()) << endl;
			publish_current_msg(t);
			}
		);
		}

private:

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

	void check_pending_handshakes(const caf::actor& p)
		{
		auto it = hs_pending.find(p);

		if ( it != hs_pending.end() )
			{
			for ( auto& o : it->second )
				{
				send(o, caf::atom("done"));
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

		bootstrap = (
		after(chrono::seconds(0)) >> [=]
			{
			try_connect(addr, port, local);
			}
		);

		disconnected = (
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
		on(atom("quit")) >> [=]
			{
			send(local, atom("unpeer"), remote);
			quit();
			},
		on(atom("handshake"), any_vals) >> [=]
			{
			forward_to(remote);
			},
		on_arg_match >> [=](down_msg d)
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
		send(local, atom("peer"), remote);
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
		{ }

	~impl()
		{
		caf::anon_send(actor, caf::atom("quit"));

		for ( const auto& p : peers )
			if ( p.remote() )
				caf::anon_send(p.pimpl->peer_actor, caf::atom("quit"));
		}

	std::string name;
	caf::actor actor;
	std::unordered_set<peering> peers;
	int last_errno;
	std::string last_error;
};
} // namespace broker

#endif // BROKER_ENDPOINT_IMPL_HH
