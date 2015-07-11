#ifndef BROKER_STORE_CLONE_IMPL_HH
#define BROKER_STORE_CLONE_IMPL_HH

#include "../atoms.hh"
#include "broker/store/clone.hh"
#include "broker/store/backend.hh"
#include "broker/store/memory_backend.hh"
#include "broker/store/sqlite_backend.hh"
#include "broker/report.hh"
#include <caf/spawn.hpp>
#include <caf/send.hpp>
#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>

namespace broker { namespace store {

class clone_actor : public caf::event_based_actor {

public:

	clone_actor(const caf::actor& endpoint, identifier master_name,
	            std::chrono::microseconds resync_interval,
	            std::unique_ptr<backend> b)
		: datastore(std::move(b))
		{
		using namespace std;
		using namespace caf;

		message_handler requests {
		[=](const identifier& n, const query& q, const actor& requester)
			{
			auto r = q.process(*datastore,
			                   broker::time_point::now().value).first;

			if ( r.stat == result::status::failure )
				error(master_name, "process query", datastore->last_error());

			return make_message(this, move(r));
			}
		};

		message_handler updates {
		on(val<identifier>, any_vals) >> [=]
			{
			forward_to(master);
			},
		[=](increment_atom, const sequence_num& sn, const data& k, int64_t by,
		    double mod_time)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				{
				if ( datastore->increment(k, by, mod_time).stat !=
				     modification_result::status::success )
					error(master_name, "increment", datastore->last_error(),
					      true);
				}
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		[=](set_add_atom, const sequence_num& sn, const data& k, data& e,
		    double mod_time)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				{
				if ( datastore->add_to_set(k, std::move(e), mod_time).stat !=
				     modification_result::status::success )
					error(master_name, "add_to_set", datastore->last_error(),
					      true);
				}
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		[=](set_rem_atom, const sequence_num& sn, const data& k, const data& e,
		    double mod_time)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				{
				if ( datastore->remove_from_set(k, e, mod_time).stat !=
				     modification_result::status::success )
					error(master_name, "remove_from_set",
					      datastore->last_error(), true);
				}
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		[=](insert_atom, const sequence_num& sn, data& k, data& v)
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
		[=](insert_atom, const sequence_num& sn, data& k, data& v,
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
		[=](erase_atom, const sequence_num& sn, const data& k)
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
		[=](expire_atom, const sequence_num& sn, const data& k,
		    const expiration_time& expiry)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				{
				if ( ! datastore->expire(k, expiry) )
					fatal_error(master_name, "expire", datastore->last_error());
				}
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		 [=](clear_atom, const sequence_num& sn)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				{
				if ( ! datastore->clear() )
					fatal_error(master_name, "clear", datastore->last_error());
				}
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		[=](lpush_atom, const sequence_num& sn, const data& k,
		    broker::vector& i, double mod_time)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				{
				if ( datastore->push_left(k, std::move(i), mod_time).stat !=
				     modification_result::status::success )
					error(master_name, "push_left",
					      datastore->last_error(), true);
				}
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		[=](rpush_atom, const sequence_num& sn, const data& k,
		    broker::vector& i, double mod_time)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				{
				if ( datastore->push_right(k, std::move(i), mod_time).stat !=
				     modification_result::status::success )
					error(master_name, "push_right",
					      datastore->last_error(), true);
				}
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		 [=](lpop_atom, const sequence_num& sn, const data& k, double mod_time)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				{
				if ( datastore->pop_left(k, mod_time).first.stat !=
				     modification_result::status::success )
					error(master_name, "pop_left", datastore->last_error(),
					      true);
				}
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			},
		[=](rpop_atom, const sequence_num& sn, const data& k, double mod_time)
			{
			auto next = datastore->sequence().next();

			if ( sn == next )
				{
				if ( datastore->pop_right(k, mod_time).first.stat !=
				     modification_result::status::success )
					error(master_name, "pop_right", datastore->last_error(),
					      true);
				}
			else if ( sn > next )
				sequence_error(master_name, resync_interval);
			}
		};

		bootstrap = {
		after(chrono::seconds::zero()) >> [=]
			{
			send(this, find_master_atom::value);
			get_snapshot(chrono::seconds::zero());
			become(synchronizing);
			}
		};

		message_handler give_actor{
		[=](store_actor_atom, const identifier& n) -> actor
			{
			return this;
			}
		};

		message_handler find_master{
		[=](find_master_atom)
			{
			sync_send(endpoint, store_actor_atom::value, master_name).then(
				[=](actor& m)
					{
					if ( m )
						{
						BROKER_DEBUG("store.clone." + master_name,
						             "Located master");
						demonitor(master);
						master = move(m);
						monitor(master);
						}
					else
						{
						BROKER_DEBUG("store.clone." + master_name,
						             "Failed to locate master, will retry...");
						delayed_send(this, resync_interval,
						             find_master_atom::value);
						}
					}
			);
			},
		[=](const down_msg& d)
			{
			if ( d.source == master.address() )
				{
				demonitor(master);
				master = caf::invalid_actor;
				BROKER_DEBUG("store.clone." + master_name,
				             "master went down, trying to relocate...");
				send(this, find_master_atom::value);
				get_snapshot(resync_interval);
				}
			}
		};

		message_handler get_snap{
		[=](get_snap_atom)
			{
			pending_getsnap = false;

			if ( ! master )
				{
				get_snapshot(resync_interval);
				return;
				}

			sync_send(master, master_name, query(query::tag::snapshot),
			          this).then(
				[=](const sync_exited_msg& m)
					{
					BROKER_DEBUG("store.clone." + master_name,
					             "master went down while requesting snapshot,"
					             " will retry...");
					get_snapshot(resync_interval);
					},
				[=](actor& responder, result& r)
					{
					if ( r.stat != result::status::success ||
					     r.value.which() != result::tag::snapshot_result )
						{
						BROKER_DEBUG("store.clone." + master_name,
						             "got invalid snapshot response, retry...");
						get_snapshot(resync_interval);
						}
					else
						{
						if ( datastore->init(move(*get<snapshot>(r.value))) )
							{
							BROKER_DEBUG("store.clone." + master_name,
							             "successful init from snapshot");
							become(active);
							}
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

	caf::behavior make_behavior() override
		{
		return bootstrap;
		}

	void error(std::string master_name, std::string method_name,
	           std::string err_msg, bool fatal = false)
		{
		report::error("store.clone." + master_name, "failed to " + method_name
		              + ": " + err_msg);

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

		delayed_send(this, resync_interval, get_snap_atom::value);
		pending_getsnap = true;
		}

	void sequence_error(const identifier& master_name,
	                    const std::chrono::microseconds& resync_interval)
		{
		report::error("store.clone." + master_name, "got desynchronized");
		get_snapshot(resync_interval);
		}

	bool pending_getsnap = false;
	std::unique_ptr<backend> datastore;
	caf::actor master;
	caf::behavior bootstrap;
	caf::behavior synchronizing;
	caf::behavior active;
	caf::behavior dead;
};


class clone::impl {
public:

	impl(const caf::actor& endpoint, identifier master_name,
	     std::chrono::microseconds resync_interval,
	     std::unique_ptr<backend> b)
		{
		// TODO: rocksdb backend should also be detached, but why does
		// rocksdb::~DB then crash?
		if ( dynamic_cast<sqlite_backend*>(b.get()) )
			actor = caf::spawn<clone_actor, caf::detached>(
			  endpoint, std::move(master_name), std::move(resync_interval),
			  std::move(b));
		else
			actor = caf::spawn<clone_actor>(
			  endpoint, std::move(master_name), std::move(resync_interval),
			  std::move(b));

		self->planned_exit_reason(caf::exit_reason::user_defined);
		actor->link_to(self);
		}

	caf::scoped_actor self;
	caf::actor actor;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_CLONE_IMPL_HH
