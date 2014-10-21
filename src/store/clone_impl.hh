#ifndef BROKER_STORE_CLONE_IMPL_HH
#define BROKER_STORE_CLONE_IMPL_HH

#include "broker/store/clone.hh"
#include "broker/store/store.hh"
#include "broker/store/mem_store.hh"
#include "broker/topic.hh"
#include <caf/spawn.hpp>
#include <caf/send.hpp>
#include <caf/actor.hpp>
#include <caf/sb_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/actor_ostream.hpp>

namespace broker { namespace store {

class clone_actor : public caf::sb_actor<clone_actor> {
friend class caf::sb_actor<clone_actor>;

public:

	clone_actor(const caf::actor& endpoint, std::string topic_name,
	            std::chrono::duration<double> resync_interval)
		{
		using namespace std;
		using namespace caf;

		topic snapshot_topic{topic_name, topic::tag::store_query};

		message_handler requests {
		on_arg_match >> [=](const topic& s, const query& q,
		                    const actor& requester)
			{
			send(requester, this, q.process(datastore));
			}
		};

		message_handler updates {
		on(val<topic>, atom("insert"), val<data>, val<data>) >> [=]
			{
			forward_to(master);
			},
		on(atom("insert"), arg_match) >> [=](const sequence_num& sn,
		                                     data& k, data& v)
			{
			auto next = datastore.sequence().next();

			if ( sn == next )
				datastore.insert(move(k), move(v));
			else if ( sn > next )
				sequence_error(snapshot_topic, endpoint);
			},
		on(val<topic>, atom("erase"), val<data>) >> [=]
			{
			forward_to(master);
			},
		on(atom("erase"), arg_match) >> [=](const sequence_num& sn,
		                                    const data& k)
			{
			auto next = datastore.sequence().next();

			if ( sn == next )
				datastore.erase(k);
			else if ( sn > next )
				sequence_error(snapshot_topic, endpoint);
			},
		on(val<topic>, atom("clear")) >> [=]
			{
			forward_to(master);
			},
		on(atom("clear"), arg_match) >> [=](const sequence_num& sn)
			{
			auto next = datastore.sequence().next();

			if ( sn == next )
				datastore.clear();
			else if ( sn > next )
				sequence_error(snapshot_topic, endpoint);
			}
		};

		bootstrap = (
		after(chrono::seconds::zero()) >> [=]
			{
			send(endpoint, snapshot_topic, query(query::tag::snapshot), this);
			become(initializing);
			}
		);

		message_handler handle_snapshot{
		on_arg_match >> [=](caf::actor& responder, result& r)
			{
			if ( r.stat != result::status::success )
				delayed_send(endpoint, resync_interval, snapshot_topic,
				             query(query::tag::snapshot), this);
			else if ( r.value.which() == result::tag::snapshot_result )
				{
				demonitor(master);
				master = move(responder);
				monitor(master);
				datastore = mem_store(move(*get<snapshot>(r.value)));
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
				send(endpoint, snapshot_topic,
				     query(query::tag::snapshot), this);
				}
			}
		);
		}

private:

	void sequence_error(const topic& t, const caf::actor& endpoint)
		{
		aout(this) << "ERROR: clone '" << t.name << "' desync" << std::endl;
		send(endpoint, t, query(query::tag::snapshot), this);
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
