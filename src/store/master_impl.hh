#ifndef BROKER_STORE_MASTER_IMPL_HH
#define BROKER_STORE_MASTER_IMPL_HH

#include "../atoms.hh"
#include "broker/store/master.hh"
#include "broker/store/sqlite_backend.hh"
#include "broker/report.hh"
#include "broker/time_point.hh"
#include <caf/send.hpp>
#include <caf/spawn.hpp>
#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <unordered_map>

namespace broker { namespace store {

static inline double now()
	{ return broker::time_point::now().value; }

class master_actor : public caf::event_based_actor {

public:

	master_actor(std::unique_ptr<backend> s, identifier name)
		: datastore(std::move(s))
		{
		using namespace caf;
		using namespace std;

		init_existing_expiry_reminders = {
		after(chrono::seconds::zero()) >> [=]
			{
			if ( auto es = datastore->expiries() )
				for ( auto& entry : *es )
					expiry_reminder(name, move(entry.key), move(entry.expiry));
			else
				error(name, "expiries", datastore->last_error());

			become(serving);
			}
		};

		message_handler give_actor{
		[=](store_actor_atom, const identifier& n) -> actor
			{
			return this;
			}
		};

		message_handler requests {
		[=](const identifier&, const query& q, const actor& r)
			{
			auto current_time = now();
			auto res = q.process(*datastore, current_time);

			if ( res.first.stat == result::status::failure )
				{
				char tmp[64];
				snprintf(tmp, sizeof(tmp), "process query (tag=%d)",
				         static_cast<int>(q.type));
				error(name, tmp, datastore->last_error());
				}
			else
				{
				switch ( q.type ) {
				case query::tag::snapshot:
				    if ( clones.find(r.address()) == clones.end() )
						{
						monitor(r);
						clones[r.address()] = r;
						}
					break;
				case query::tag::pop_left:
					// fallthrough
				case query::tag::pop_right:
					if ( which(res.first.value) ==
					     result::tag::lookup_or_pop_result )
						{
						if ( res.second && res.second->new_expiration )
							expiry_reminder(name, q.k,
							                move(*res.second->new_expiration));

						if ( clones.empty() )
							break;

						if ( q.type == query::tag::pop_left )
							publish(make_message(lpop_atom::value,
							                     datastore->sequence(), q.k,
							                     current_time));
						else
							publish(make_message(rpop_atom::value,
							                     datastore->sequence(), q.k,
							                     current_time));
						}
					break;
				default:
					break;
				}
				}

			return make_message(this, move(res.first));
			},
		};

		message_handler updates {
		[=](expire_atom, data& k, expiration_time& expiry)
			{
			if ( ! datastore->expire(k, expiry) )
				{
				error(name, "expire", datastore->last_error());
				return;
				}

			BROKER_DEBUG("store.master." + name, "Expire key: " + to_string(k));

			if ( ! clones.empty() )
				publish(make_message(expire_atom::value, datastore->sequence(),
				                     move(k), move(expiry)));
			},
		[=](const identifier&, increment_atom, data& k, int64_t by)
			{
			auto mod_time = now();
			auto res = datastore->increment(k, by, mod_time);

			if ( res.stat != modification_result::status::success )
				{
				error(name, "increment", datastore->last_error());
				return;
				}

			if ( res.new_expiration )
				expiry_reminder(name, k, move(*res.new_expiration));

			if ( ! clones.empty() )
				publish(make_message(increment_atom::value, datastore->sequence(),
				                     move(k), by, mod_time));
			},
		[=](const identifier&, set_add_atom, data& k, data& e)
			{
			auto mod_time = now();
			auto res = datastore->add_to_set(k, clones.empty() ? move(e) : e,
			                                 mod_time);

			if ( res.stat != modification_result::status::success )
				{
				error(name, "add_to_set", datastore->last_error());
				return;
				}

			if ( res.new_expiration )
				expiry_reminder(name, k, move(*res.new_expiration));

			if ( ! clones.empty() )
				publish(make_message(set_add_atom::value, datastore->sequence(),
				                     move(k), move(e), mod_time));
			},
		[=](const identifier&, set_rem_atom, data& k, data& e)
			{
			auto mod_time = now();
			auto res = datastore->remove_from_set(k, e, mod_time);

			if ( res.stat != modification_result::status::success )
				{
				error(name, "remove_from_set", datastore->last_error());
				return;
				}

			if ( res.new_expiration )
				expiry_reminder(name, k, move(*res.new_expiration));

			if ( ! clones.empty() )
				publish(make_message(set_rem_atom::value, datastore->sequence(),
				                     move(k), move(e), mod_time));
			},
		[=](const identifier&, insert_atom, data& k, data& v)
			{
			if ( ! datastore->insert(clones.empty() ? move(k) : k,
			                         clones.empty() ? move(v) : v) )
				{
				error(name, "insert", datastore->last_error());
				return;
				}

			if ( ! clones.empty() )
				publish(make_message(insert_atom::value, datastore->sequence(),
				                     move(k), move(v)));
			},
		[=](const identifier&, insert_atom, data& k, data& v, expiration_time t)
			{
			if ( t.type == expiration_time::tag::absolute &&
			     t.expiry_time <= now() )
				return;

			if ( ! datastore->insert(k, clones.empty() ? move(v) : v, t) )
				{
				error(name, "insert_with_expiry", datastore->last_error());
				return;
				}

			if ( clones.empty() )
				expiry_reminder(name, move(k), t);
			else
				{
				expiry_reminder(name, k, t);
				publish(make_message(insert_atom::value, datastore->sequence(),
				                     move(k), move(v), t));
				}
			},
		[=](const identifier&, erase_atom, data& k)
			{
			if ( ! datastore->erase(k) )
				{
				error(name, "erase", datastore->last_error());
				return;
				}

			if ( ! clones.empty() )
				publish(make_message(erase_atom::value, datastore->sequence(),
				                     move(k)));
			},
		[=](const identifier&, clear_atom)
			{
			if ( ! datastore->clear() )
				{
				error(name, "clear", datastore->last_error());
				return;
				}

			if ( ! clones.empty() )
				publish(make_message(clear_atom::value, datastore->sequence()));
			},
		[=](const identifier&, lpush_atom, data& k, vector& i)
			{
			auto mod_time = now();
			auto res = datastore->push_left(k, clones.empty() ? move(i) : i,
			                                mod_time);

			if ( res.stat != modification_result::status::success )
				{
				error(name, "push_left", datastore->last_error());
				return;
				}

			if ( res.new_expiration )
				expiry_reminder(name, k, move(*res.new_expiration));

			if ( ! clones.empty() )
				publish(make_message(lpush_atom::value, datastore->sequence(),
				                     move(k), move(i), mod_time));
			},
		[=](const identifier&, rpush_atom, data& k, vector& i)
			{
			auto mod_time = now();
			auto res = datastore->push_right(k, clones.empty() ? move(i) : i,
			                                 mod_time);

			if ( res.stat != modification_result::status::success )
				{
				error(name, "push_right", datastore->last_error());
				return;
				}

			if ( res.new_expiration )
				expiry_reminder(name, k, move(*res.new_expiration));

			if ( ! clones.empty() )
				publish(make_message(rpush_atom::value, datastore->sequence(),
				                     move(k), move(i), mod_time));
			}
		};

		serving = requests.or_else(updates).or_else(give_actor).or_else(
		[=](const down_msg& d)
			{
			demonitor(d.source);
			clones.erase(d.source);
			}
		);
		}

private:

	caf::behavior make_behavior() override
		{
		return init_existing_expiry_reminders;
		}

	void expiry_reminder(const identifier& name, data key,
	                     expiration_time expiry)
		{
		using namespace std::chrono;
		double abs_expire_time = 0;

		switch ( expiry.type ) {
		case expiration_time::tag::absolute:
			abs_expire_time = expiry.expiry_time;
			break;
		case expiration_time::tag::since_last_modification:
			abs_expire_time = expiry.expiry_time + expiry.modification_time;
			break;
		default:
			assert(! "bad expiry type");
		}

		double wait_secs = std::max(0.0, abs_expire_time - now());
		BROKER_DEBUG("store.master." + name,
		             "Send reminder to expire key: " + to_string(key) + " in " +
		             to_string(data{wait_secs}) + " seconds");
		delayed_send(this,
		             duration_cast<microseconds>(duration<double>(wait_secs)),
		             expire_atom::value, std::move(key), std::move(expiry));
		}

	void publish(caf::message msg)
		{
		for ( const auto& c : clones ) send(c.second, msg);
		}

	void error(std::string master_name, std::string method_name,
	           std::string err_msg)
		{
		report::error("store.master." + master_name, "failed to " + method_name
		              + ": " + err_msg);
		}

	std::unique_ptr<backend> datastore;
	std::unordered_map<caf::actor_addr, caf::actor> clones;
	caf::behavior serving;
	caf::behavior init_existing_expiry_reminders;
};

class master::impl {
public:

	impl(const caf::actor& endpoint, identifier name,
	     std::unique_ptr<backend> s)
		{
		// TODO: rocksdb backend should also be detached, but why does
		// rocksdb::~DB then crash?
		if ( dynamic_cast<sqlite_backend*>(s.get()) )
			actor = caf::spawn<master_actor, caf::detached>(std::move(s), name);
		else
			actor = caf::spawn<master_actor>(std::move(s), name);

		self->planned_exit_reason(caf::exit_reason::user_defined);
		actor->link_to(self);
		caf::anon_send(endpoint, master_atom::value, std::move(name), actor);
		}

	caf::scoped_actor self;
	caf::actor actor;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_MASTER_IMPL_HH
