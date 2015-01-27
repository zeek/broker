#ifndef BROKER_STORE_MASTER_IMPL_HH
#define BROKER_STORE_MASTER_IMPL_HH

#include "broker/store/master.hh"
#include "broker/report.hh"
#include "broker/time_point.hh"
#include <caf/send.hpp>
#include <caf/spawn.hpp>
#include <caf/actor.hpp>
#include <caf/sb_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <unordered_map>

namespace broker { namespace store {

static inline double now()
	{ return broker::time_point::now().value; }

class timer_actor : public caf::sb_actor<timer_actor> {
friend class caf::sb_actor<timer_actor>;

public:

	timer_actor(data key, expiration_time t, caf::actor master)
		{
		using namespace std::chrono;

		microseconds wait(0);
		double n = now();

		if ( t.type == expiration_time::tag::absolute && t.time > n )
			wait = duration_cast<microseconds>(duration<double>(t.time - n));
		else
			wait = duration_cast<microseconds>(duration<double>(t.time));

		timing = (
		caf::on(caf::atom("quit")) >> [=]
			{
			quit();
			},
		caf::on(caf::atom("refresh")) >> [=]
			{
			// Cause after() handler to wait another full timeout period.
			},
		caf::after(wait) >> [=]
			{
			send(master, caf::atom("expire"), std::move(key));
			quit();
			}
		);
		}

private:

	caf::behavior timing;
	caf::behavior& init_state = timing;
};

class timer {
public:

	timer() = default;

	timer(timer&&) = default;

	timer(const timer&) = delete;

	timer& operator=(timer&&) = default;

	timer& operator=(const timer&) = delete;

	timer(data key, expiration_time t, caf::actor master)
		: expiry(t),
		  actor(caf::spawn<timer_actor>(std::move(key), std::move(t),
		                                std::move(master)))
		{}

	~timer()
		{ caf::anon_send(actor, caf::atom("quit")); }

	expiration_time expiry;
	caf::actor actor;
};

class master_actor : public caf::sb_actor<master_actor> {
friend class caf::sb_actor<master_actor>;

public:

	master_actor(std::unique_ptr<backend> s, identifier name)
		: datastore(std::move(s))
		{
		using namespace caf;
		using namespace std;

		init_timers = (
		after(chrono::seconds::zero()) >> [=]
			{
			if ( auto es = datastore->expiries() )
				for ( auto& entry : *es )
					timers[entry.key] = timer(move(entry.key), entry.expiry,
					                          this);
			else
				error(name, "expiries", datastore->last_error());

			become(serving);
			}
		);

		message_handler give_actor{
		on(atom("storeactor"), arg_match) >> [=](const identifier& n) -> actor
			{
			return this;
			}
		};

		message_handler requests {
		on(val<identifier>, arg_match) >> [=](const query& q, const actor& r)
			{
			auto res = q.process(*datastore);

			if ( res.stat == result::status::failure )
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
					if ( which(res.value) == result::tag::lookup_or_pop_result )
						{
						refresh_modification_time(q.k);

						if ( clones.empty() )
							break;

						auto op = q.type == query::tag::pop_left ? atom("lpop")
						                                         : atom("rpop");
						publish(make_message(op, datastore->sequence(), q.k));
						}
					break;
				default:
					break;
				}
				}

			return make_message(this, move(res));
			},
		};

		message_handler updates {
		on(atom("expire"), arg_match) >> [=](const data& k)
			{
			if ( ! datastore->erase(k) )
				{
				error(name, "expire/erase", datastore->last_error());
				return;
				}

			BROKER_DEBUG("store.master." + name, "Expire key: " + to_string(k));
			timers.erase(k);

			if ( ! clones.empty() )
				publish(make_message(atom("erase"), datastore->sequence(),
				                     move(k)));
			},
		on(val<identifier>, atom("increment"), arg_match) >> [=](data& k,
		                                                         int64_t by)
			{
			if ( datastore->increment(k, by) != 0 )
				{
				error(name, "increment", datastore->last_error());
				return;
				}

			refresh_modification_time(k);

			if ( ! clones.empty() )
				publish(make_message(atom("increment"), datastore->sequence(),
				                     move(k), by));
			},
		on(val<identifier>, atom("set_add"), arg_match) >> [=](data& k, data& e)
			{
			if ( datastore->add_to_set(k, clones.empty() ? move(e) : e) != 0 )
				{
				error(name, "add_to_set", datastore->last_error());
				return;
				}

			refresh_modification_time(k);

			if ( ! clones.empty() )
				publish(make_message(atom("set_add"), datastore->sequence(),
				                     move(k), move(e)));
			},
		on(val<identifier>, atom("set_rem"), arg_match) >> [=](data& k, data& e)
			{
			if ( datastore->remove_from_set(k, e) != 0 )
				{
				error(name, "remove_from_set", datastore->last_error());
				return;
				}

			refresh_modification_time(k);

			if ( ! clones.empty() )
				publish(make_message(atom("set_rem"), datastore->sequence(),
				                     move(k), move(e)));
			},
		on(val<identifier>, atom("insert"), arg_match) >> [=](data& k, data& v)
			{
			timers.erase(k);

			if ( ! datastore->insert(clones.empty() ? move(k) : k,
			                         clones.empty() ? move(v) : v) )
				{
				error(name, "insert", datastore->last_error());
				return;
				}

			if ( ! clones.empty() )
				publish(make_message(atom("insert"), datastore->sequence(),
				                     move(k), move(v)));
			},
		on(val<identifier>, atom("insert"), arg_match) >> [=](data& k, data& v,
		                                                      expiration_time t)
			{
			if ( t.type == expiration_time::tag::absolute && t.time <= now() )
				return;

			timers[k] = timer(k, t, this);

			if ( ! datastore->insert(clones.empty() ? move(k) : k,
			                         clones.empty() ? move(v) : v) )
				{
				error(name, "insert_with_expiry", datastore->last_error());
				return;
				}

			if ( ! clones.empty() )
				publish(make_message(atom("insert"), datastore->sequence(),
				                     move(k), move(v), t));
			},
		on(val<identifier>, atom("erase"), arg_match) >> [=](data& k)
			{
			if ( ! datastore->erase(k) )
				{
				error(name, "erase", datastore->last_error());
				return;
				}

			timers.erase(k);

			if ( ! clones.empty() )
				publish(make_message(atom("erase"), datastore->sequence(),
				                     move(k)));
			},
		on(val<identifier>, atom("clear"), arg_match) >> [=]
			{
			if ( ! datastore->clear() )
				{
				error(name, "clear", datastore->last_error());
				return;
				}

			timers.clear();

			if ( ! clones.empty() )
				publish(make_message(atom("clear"), datastore->sequence()));
			},
		on(val<identifier>, atom("lpush"), arg_match) >> [=](data& k, vector& i)
			{
			if ( datastore->push_left(k, clones.empty() ? move(i) : i) != 0 )
				{
				error(name, "push_left", datastore->last_error());
				return;
				}

			refresh_modification_time(k);

			if ( ! clones.empty() )
				publish(make_message(atom("lpush"), datastore->sequence(),
				                     move(k), move(i)));
			},
		on(val<identifier>, atom("rpush"), arg_match) >> [=](data& k, vector& i)
			{
			if ( datastore->push_right(k, clones.empty() ? move(i) : i) != 0 )
				{
				error(name, "push_right", datastore->last_error());
				return;
				}

			refresh_modification_time(k);

			if ( ! clones.empty() )
				publish(make_message(atom("rpush"), datastore->sequence(),
				                     move(k), move(i)));
			}
		};

		serving = requests.or_else(updates).or_else(give_actor).or_else(
		on_arg_match >> [=](const down_msg& d)
			{
			demonitor(d.source);
			clones.erase(d.source);
			}
		);
		}

private:

	void refresh_modification_time(const data& key)
		{
		auto it = timers.find(key);

		if ( it == timers.end() )
			return;

		const timer& t = it->second;

		if ( t.expiry.type == expiration_time::tag::since_last_modification )
			caf::anon_send(t.actor, caf::atom("refresh"));
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
	std::unordered_map<data, timer> timers;
	std::unordered_map<caf::actor_addr, caf::actor> clones;
	caf::behavior serving;
	caf::behavior init_timers;
	caf::behavior& init_state = init_timers;
};

class master::impl {
public:

	impl(const caf::actor& endpoint, identifier name,
	     std::unique_ptr<backend> s)
		: self(), actor(caf::spawn<master_actor>(std::move(s), name))
		{
		self->planned_exit_reason(caf::exit_reason::user_defined);
		actor->link_to(self);
		caf::anon_send(endpoint, caf::atom("master"), std::move(name), actor);
		}

	caf::scoped_actor self;
	caf::actor actor;
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_MASTER_IMPL_HH
