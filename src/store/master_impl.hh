#ifndef BROKER_STORE_MASTER_IMPL_HH
#define BROKER_STORE_MASTER_IMPL_HH

#include "broker/store/master.hh"
#include "broker/store/store.hh"
#include <caf/send.hpp>
#include <caf/spawn.hpp>
#include <caf/actor.hpp>
#include <caf/sb_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/actor_ostream.hpp>

namespace broker { namespace store {

class master_actor : public caf::sb_actor<master_actor> {
friend class caf::sb_actor<master_actor>;

public:

	master_actor(std::unique_ptr<store> s, identifier name)
		: datastore(std::move(s))
		{
		using namespace caf;
		using namespace std;

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
		on(val<identifier>, atom("insert"), arg_match) >> [=](data& k, data& v)
			{
			datastore->insert(k, v);

			if ( ! clones.empty() )
				publish(make_message(atom("insert"), datastore->sequence(),
				                     move(k), move(v)));
			},
		on(val<identifier>, atom("erase"), arg_match) >> [=](data& k)
			{
			datastore->erase(k);

			if ( ! clones.empty() )
				publish(make_message(atom("erase"), datastore->sequence(),
				                     move(k)));
			},
		on(val<identifier>, atom("clear"), arg_match) >> [=]
			{
			datastore->clear();

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

	std::unique_ptr<store> datastore;
	std::unordered_map<caf::actor_addr, caf::actor> clones;
	caf::behavior serving;
	caf::behavior& init_state = serving;
};

class master::impl {
public:

	impl(const caf::actor& endpoint, identifier name,
	     std::unique_ptr<store> s)
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
