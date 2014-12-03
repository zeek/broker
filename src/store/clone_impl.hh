#ifndef BROKER_STORE_CLONE_IMPL_HH
#define BROKER_STORE_CLONE_IMPL_HH

#include "broker/store/clone.hh"
#include "broker/store/backend.hh"
#include "broker/store/memory_backend.hh"
#include "broker/report.hh"
#include <caf/spawn.hpp>
#include <caf/send.hpp>
#include <caf/actor.hpp>
#include <caf/sb_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <sstream>

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
			auto r = q.process(*datastore);

			if ( r.stat == result::status::failure )
				error(master_name, "process query", datastore->last_error());

			return make_message(this, move(r));
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
				{
				int rc;

				if ( (rc = datastore->increment(k, by)) != 0 )
					error(master_name, "increment", datastore->last_error(),
					      rc < 0);
				}
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		on(val<identifier>, atom("set_add"), any_vals) >> [=]
			{
			forward_to(master);
			},
		on(atom("set_add"), arg_match) >> [=](const sequence_num& sn,
		                                      const data& k, data& e)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				{
				int rc;

				if ( (rc = datastore->add_to_set(k, std::move(e))) != 0 )
					error(master_name, "add_to_set", datastore->last_error(),
					      rc < 0);
				}
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		on(val<identifier>, atom("set_rem"), any_vals) >> [=]
			{
			forward_to(master);
			},
		on(atom("set_rem"), arg_match) >> [=](const sequence_num& sn,
		                                      const data& k, const data& e)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				{
				int rc;

				if ( (rc = datastore->remove_from_set(k, e)) != 0 )
					error(master_name, "remove_from_set",
					      datastore->last_error(), rc < 0);
				}
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
				{
				if ( ! datastore->insert(move(k), move(v)) )
					fatal_error(master_name, "insert", datastore->last_error());
				}
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		on(atom("insert"), arg_match) >> [=](const sequence_num& sn,
		                                     data& k, data& v,
		                                     expiration_time t)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				{
				if ( ! datastore->insert(move(k), move(v), t) )
					fatal_error(master_name, "insert_with_expiry",
					            datastore->last_error());
				}
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
				{
				if ( ! datastore->erase(k) )
					fatal_error(master_name, "erase", datastore->last_error());
				}
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
				{
				if ( ! datastore->clear() )
					fatal_error(master_name, "clear", datastore->last_error());
				}
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
						if ( datastore->init(move(*get<snapshot>(r.value))) )
							become(active);
						else
							fatal_error(master_name, "init",
							            datastore->last_error());
						}
					}
			);
			}
		};

		auto handlesync = find_master.or_else(get_snap).or_else(give_actor);
		synchronizing = handlesync;
		active = requests.or_else(updates).or_else(handlesync);
		dead = requests.or_else(give_actor).or_else(others() >> []{});
		}

private:

	void error(std::string master_name, std::string method_name,
	           std::string err_msg, bool fatal = false)
		{
		std::ostringstream msg;
		msg << "Clone of '" << master_name << "' failed to "
		    << method_name << ": " << err_msg;
		report::error("data.clone." + master_name, msg.str());

		if ( fatal )
			become(dead);
		}

	void fatal_error(std::string master_name, std::string method_name,
	                 std::string err_msg)
		{
		error(master_name, method_name, err_msg, true);
		}

	void get_snapshot(const std::chrono::microseconds& resync_interval)
		{
		if ( pending_getsnap )
			return;

		delayed_send(this, resync_interval, caf::atom("getsnap"));
		pending_getsnap = true;
		}

	void sequence_error(const identifier& master_name,
	                    const std::chrono::microseconds& resync_interval)
		{
		std::ostringstream msg;
		msg << "Clone of '" << master_name << "' got desynchronized.";
		report::error("data.clone." + master_name, msg.str());
		get_snapshot(resync_interval);
		}

	bool pending_getsnap = false;
	std::unique_ptr<backend> datastore;
	caf::actor master;
	caf::behavior bootstrap;
	caf::behavior synchronizing;
	caf::behavior active;
	caf::behavior dead;
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
