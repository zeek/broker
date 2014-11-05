#ifndef BROKER_STORE_MASTER_IMPL_HH
#define BROKER_STORE_MASTER_IMPL_HH

#include "broker/store/master.hh"
#include <caf/send.hpp>
#include <caf/spawn.hpp>
#include <caf/actor.hpp>
#include <caf/sb_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/actor_ostream.hpp>
#include <sys/time.h>

namespace broker { namespace store {

static double now()
	{
	struct timeval tv;
	gettimeofday(&tv, 0);
	return tv.tv_sec + (tv.tv_usec / 1000000.0);
	}

class timer_actor : public caf::sb_actor<timer_actor> {
friend class caf::sb_actor<timer_actor>;

public:

	timer_actor(data key, expiration_time t, caf::actor master)
		{
		using namespace caf;

		double wait_time = 0.0;
		double n = now();

		if ( t.type == expiration_time::tag::absolute && t.time > n )
			wait_time = t.time - n;
		else
			wait_time = t.time;

		timing = (
		on(atom("quit")) >> [=]
			{
			quit();
			},
		on(atom("refresh")) >> [=]
			{
			},
		after(std::chrono::duration<double>(wait_time)) >> [=]
			{
			send(master, atom("expire"), std::move(key));
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
			for ( auto& entry : datastore->expiries() )
				timers[entry.key] = timer(move(entry.key), entry.expiry, this);

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
			if ( q.type == query::tag::snapshot &&
			     clones.find(r.address()) == clones.end() )
				{
				monitor(r);
				clones[r.address()] = r;
				}

			return make_message(this, q.process(*datastore.get()));
			},
		};

		message_handler updates {
		on(atom("expire"), arg_match) >> [=](const data& k)
			{
			datastore->erase(k);
			timers.erase(k);

			if ( ! clones.empty() )
				publish(make_message(atom("erase"), datastore->sequence(),
				                     move(k)));
			},
		on(val<identifier>, atom("insert"), arg_match) >> [=](data& k, data& v)
			{
			timers.erase(k);

			if ( clones.empty() )
				datastore->insert(move(k), move(v));
			else
				{
				datastore->insert(k, v);
				publish(make_message(atom("insert"), datastore->sequence(),
				                     move(k), move(v)));
				}
			},
		on(val<identifier>, atom("insert"), arg_match) >> [=](data& k, data& v,
		                                                      expiration_time t)
			{
			if ( t.type == expiration_time::tag::absolute && t.time <= now() )
				return;

			timers[k] = timer(k, t, this);

			if ( clones.empty() )
				datastore->insert(move(k), move(v), t);
			else
				{
				datastore->insert(k, v, t);
				publish(make_message(atom("insert"), datastore->sequence(),
				                     move(k), move(v), t));
				}
			},
		on(val<identifier>, atom("erase"), arg_match) >> [=](data& k)
			{
			datastore->erase(k);
			timers.erase(k);

			if ( ! clones.empty() )
				publish(make_message(atom("erase"), datastore->sequence(),
				                     move(k)));
			},
		on(val<identifier>, atom("clear"), arg_match) >> [=]
			{
			datastore->clear();
			timers.clear();

			if (! clones.empty() )
				publish(make_message(atom("clear"), datastore->sequence()));
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

	void publish(caf::message msg)
		{
		for ( const auto& c : clones ) send_tuple(c.second, msg);
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
