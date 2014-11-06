#ifndef BROKER_STORE_CLONE_IMPL_HH
#define BROKER_STORE_CLONE_IMPL_HH

#include "broker/store/clone.hh"
#include "broker/store/backend.hh"
#include "broker/store/memory_backend.hh"
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

	clone_actor(const caf::actor& endpoint, identifier master_name,
	            std::chrono::microseconds resync_interval,
	            std::unique_ptr<backend> b)
		: datastore(std::move(b))
		{
		using namespace std;
		using namespace caf;

		message_handler requests {
		on_arg_match >> [=](const identifier& n, const query& q,
		                    const actor& requester)
			{
			return make_message(this, q.process(*datastore));
			}
		};

		message_handler updates {
		on(val<identifier>, atom("increment"), any_vals) >> [=]
			{
			forward_to(master);
			},
		on(atom("increment"), arg_match) >> [=](const sequence_num& sn,
		                                        const data& k, int64_t by)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				datastore->increment(k, by);
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		on(val<identifier>, atom("insert"), any_vals) >> [=]
			{
			forward_to(master);
			},
		on(atom("insert"), arg_match) >> [=](const sequence_num& sn,
		                                     data& k, data& v)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				datastore->insert(move(k), move(v));
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		on(atom("insert"), arg_match) >> [=](const sequence_num& sn,
		                                     data& k, data& v,
		                                     expiration_time t)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				datastore->insert(move(k), move(v), t);
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		on(val<identifier>, atom("erase"), val<data>) >> [=]
			{
			forward_to(master);
			},
		on(atom("erase"), arg_match) >> [=](const sequence_num& sn,
		                                    const data& k)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				datastore->erase(k);
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		on(val<identifier>, atom("clear")) >> [=]
			{
			forward_to(master);
			},
		on(atom("clear"), arg_match) >> [=](const sequence_num& sn)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				datastore->clear();
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			}
		};

		bootstrap = (
		after(chrono::seconds::zero()) >> [=]
			{
			send(this, atom("findmaster"));
			get_snapshot(chrono::seconds::zero());
			become(synchronizing);
			}
		);

		message_handler give_actor{
		on(atom("storeactor"), arg_match) >> [=](const identifier& n) -> actor
			{
			return this;
			}
		};

		message_handler find_master{
		on(atom("findmaster")) >> [=]
			{
			sync_send(endpoint, atom("storeactor"), master_name).then(
				on_arg_match >> [=](actor& m)
					{
					if ( m )
						{
						demonitor(master);
						master = move(m);
						monitor(master);
						}
					else
						delayed_send(this, resync_interval, atom("findmaster"));
					}
			);
			},
		on_arg_match >> [=](const down_msg& d)
			{
			if ( d.source == master.address() )
				{
				send(this, atom("findmaster"));
				get_snapshot(resync_interval);
				}
			}
		};

		message_handler get_snap{
		on(atom("getsnap")) >> [=]
			{
			pending_getsnap = false;

			if ( ! master )
				{
				get_snapshot(resync_interval);
				return;
				}

			sync_send(master, master_name, query(query::tag::snapshot),
			          this).then(
				on_arg_match >> [=](const sync_exited_msg& m)
					{
					get_snapshot(resync_interval);
					},
				on_arg_match >> [=](actor& responder, result& r)
					{
					if ( r.stat != result::status::success ||
					     r.value.which() != result::tag::snapshot_result )
						get_snapshot(resync_interval);
					else
						{
						datastore->init(move(*get<snapshot>(r.value)));
						become(active);
						}
					}
			);
			}
		};

		auto handlesync = find_master.or_else(get_snap).or_else(give_actor);
		synchronizing = handlesync;
		active = requests.or_else(updates).or_else(handlesync);
		}

private:

	void get_snapshot(const std::chrono::microseconds& resync_interval)
		{
		if ( pending_getsnap )
			return;

		delayed_send(this, resync_interval, caf::atom("getsnap"));
		pending_getsnap = true;
		}

	void sequence_error(const identifier& id,
	                    const std::chrono::microseconds& resync_interval)
		{
		aout(this) << "ERROR: clone '" << id << "' desync" << std::endl;
		get_snapshot(resync_interval);
		}

	bool pending_getsnap = false;
	std::unique_ptr<backend> datastore;
	caf::actor master;
	caf::behavior bootstrap;
	caf::behavior synchronizing;
	caf::behavior active;
	caf::behavior& init_state = bootstrap;
};


class clone::impl {
public:

	impl(const caf::actor& endpoint, identifier master_name,
	     std::chrono::microseconds resync_interval,
	     std::unique_ptr<backend> b)
		: self(), actor(caf::spawn<clone_actor>(endpoint,
	                                            std::move(master_name),
	                                            std::move(resync_interval),
	                                            std::move(b)))
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
