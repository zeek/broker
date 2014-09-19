#ifndef BROKER_STORE_CLONE_IMPL_HH
#define BROKER_STORE_CLONE_IMPL_HH

#include "broker/store/clone.hh"
#include "broker/store/store.hh"
#include "broker/store/mem_store.hh"
#include "../subscription.hh"
#include <caf/spawn.hpp>
#include <caf/send.hpp>
#include <caf/actor.hpp>
#include <caf/sb_actor.hpp>
#include <caf/actor_ostream.hpp>

namespace broker { namespace store {

class clone_actor : public caf::sb_actor<clone_actor> {
friend class caf::sb_actor<clone_actor>;

public:

	clone_actor(const caf::actor& endpoint, std::string topic,
	            std::chrono::duration<double> resync_interval)
		{
		using namespace std;
		using namespace caf;

		subscription snap_topic{subscription_type::store_query, topic};

		message_handler requests {
		on_arg_match >> [=](const subscription& s, const query& q,
		                    const actor& requester)
			{
			send(requester, this, q.process(datastore));
			}
		};

		message_handler updates {
		on(val<subscription>, atom("insert"), val<key>, val<value>) >> [=]
			{
			forward_to(master);
			},
		on(atom("insert"), arg_match) >> [=](const sequence_num& sn,
		                                     key& k, value& v)
			{
			auto next = datastore.sequence().next();

			if ( sn == next )
				datastore.insert(move(k), move(v));
			else if ( sn > next )
				sequence_error(snap_topic, endpoint);
			},
		on(val<subscription>, atom("erase"), val<key>) >> [=]
			{
			forward_to(master);
			},
		on(atom("erase"), arg_match) >> [=](const sequence_num& sn,
		                                    const key& k)
			{
			auto next = datastore.sequence().next();

			if ( sn == next )
				datastore.erase(k);
			else if ( sn > next )
				sequence_error(snap_topic, endpoint);
			},
		on(val<subscription>, atom("clear")) >> [=]
			{
			forward_to(master);
			},
		on(atom("clear"), arg_match) >> [=](const sequence_num& sn)
			{
			auto next = datastore.sequence().next();

			if ( sn == next )
				datastore.clear();
			else if ( sn > next )
				sequence_error(snap_topic, endpoint);
			}
		};

		bootstrap = (
		after(chrono::seconds::zero()) >> [=]
			{
			send(endpoint, snap_topic, query(query::type::snapshot), this);
			become(initializing);
			}
		);

		message_handler handle_snapshot{
		on_arg_match >> [=](caf::actor& responder, result& r)
			{
			if ( r.stat != result::status::success )
				delayed_send(endpoint, resync_interval, snap_topic,
				             query(query::type::snapshot), this);
			else if ( r.tag == result::type::snapshot_val )
				{
				demonitor(master);
				master = move(responder);
				monitor(master);
				datastore = mem_store(move(r.snap));
				become(active);
				}
			}
		};

		initializing = handle_snapshot;

		active = requests.or_else(updates).or_else(handle_snapshot).or_else(
		on_arg_match >> [=](const down_msg& d)
			{
			if ( d.source == master.address() )
				{
				demonitor(master);
				master = invalid_actor;
				send(endpoint, snap_topic, query(query::type::snapshot), this);
				}
			}
		);
		}

private:

	void sequence_error(const subscription& sub, const caf::actor& endpoint)
		{
		aout(this) << "ERROR: clone '" << sub.topic << "' desync" << std::endl;
		send(endpoint, sub, query(query::type::snapshot), this);
		}

	mem_store datastore;
	caf::actor master;
	caf::behavior bootstrap;
	caf::behavior initializing;
	caf::behavior active;
	caf::behavior& init_state = bootstrap;
};


class clone::impl {
public:

	impl(const caf::actor& endpoint, std::string topic,
	     std::chrono::duration<double> resync_interval)
		: self(), actor(caf::spawn<clone_actor>(endpoint, std::move(topic),
	                                            std::move(resync_interval)))
		{
		self->planned_exit_reason(caf::exit_reason::user_defined);
		actor->link_to(self);
		}

	caf::scoped_actor self;
	caf::actor actor;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_CLONE_IMPL_HH
