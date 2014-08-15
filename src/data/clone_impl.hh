#ifndef BROKER_DATA_CLONE_IMPL_HH
#define BROKER_DATA_CLONE_IMPL_HH

#include "broker/data/clone.hh"
#include "broker/data/store.hh"
#include "broker/data/mem_store.hh"
#include "../subscription.hh"
#include <caf/spawn.hpp>
#include <caf/send.hpp>
#include <caf/actor.hpp>
#include <caf/sb_actor.hpp>
#include <caf/actor_ostream.hpp>

namespace broker { namespace data {

class clone_actor : public caf::sb_actor<clone_actor> {
friend class caf::sb_actor<clone_actor>;

public:

	clone_actor(const caf::actor& endpoint, std::string topic)
		{
		using namespace std;
		using namespace caf;

		// TODO: expose this retry interval to user API
		auto sync_retry_interval = chrono::seconds(5);
		subscription update_topic{subscription_type::data_update, topic};
		subscription snap_topic{subscription_type::data_query, topic};

		message_handler requests {
		on_arg_match >> [=](const subscription& s, const query& q,
		                    const actor& requester)
			{
			send(requester, this, q.process(datastore));
			}
		};

		message_handler updates {
		on(update_topic, atom("insert"), val<key>, val<value>) >> [=]
			{
			forward_to(master);
			},
		on(atom("insert"), arg_match) >> [=](sequence_num sn, key k, value v)
			{
			auto next = datastore.sequence().next();

			if ( sn == next )
				datastore.insert(move(k), move(v));
			else if ( sn > next )
				sequence_error(snap_topic, endpoint);
			},
		on(update_topic, atom("erase"), val<key>) >> [=]
			{
			forward_to(master);
			},
		on(atom("erase"), arg_match) >> [=](sequence_num sn, key k)
			{
			auto next = datastore.sequence().next();

			if ( sn == next )
				datastore.erase(k);
			else if ( sn > next )
				sequence_error(snap_topic, endpoint);
			},
		on(update_topic, atom("clear")) >> [=]
			{
			forward_to(master);
			},
		on(atom("clear"), arg_match) >> [=](sequence_num sn)
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
			if ( r.tag != result::type::snapshot_val )
				return;

			if ( r.stat != result::status::success )
				delayed_send(endpoint, sync_retry_interval, snap_topic,
				             query(query::type::snapshot), this);
			else
				{
				demonitor(master);
				master = responder;
				monitor(master);
				datastore = mem_store(move(r.snap));
				}

			become(active);
			}
		};

		initializing = handle_snapshot;

		active = requests.or_else(updates).or_else(handle_snapshot).or_else(
		on_arg_match >> [=](down_msg d)
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

	impl(const caf::actor& endpoint, std::string topic)
		: actor(caf::spawn<clone_actor>(endpoint, std::move(topic)))
		{ }

	~impl()
		{
		caf::anon_send(actor, caf::atom("quit"));
		}

	caf::actor actor;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_CLONE_IMPL_HH
