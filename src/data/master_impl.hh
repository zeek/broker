#ifndef BROKER_DATA_MASTER_IMPL_HH
#define BROKER_DATA_MASTER_IMPL_HH

#include "broker/data/master.hh"
#include "broker/data/store.hh"
#include "../subscription.hh"
#include <caf/send.hpp>
#include <caf/spawn.hpp>
#include <caf/actor.hpp>
#include <caf/sb_actor.hpp>
#include <caf/actor_ostream.hpp>

namespace broker { namespace data {

class master_actor : public caf::sb_actor<master_actor> {
friend class caf::sb_actor<master_actor>;

public:

	master_actor(std::unique_ptr<store> s, std::string topic)
		: datastore(std::move(s))
		{
		using namespace caf;
		using namespace std;

		subscription update_topic{subscription_type::data_update, topic};

		message_handler requests {
		on(val<subscription>, arg_match) >> [=](const query& q, const actor& r)
			{
			if ( q.tag == query::type::snapshot &&
			     clones.find(r.address()) == clones.end() )
				{
				monitor(r);
				clones[r.address()] = r;
				}

			send(r, this, q.process(*datastore.get()));
			},
		};

		message_handler updates {
		on(update_topic, atom("insert"), arg_match) >> [=](key k, value v)
			{
			datastore->insert(k, v);

			if ( ! clones.empty() )
				publish(make_message(atom("insert"), datastore->sequence(),
				                     move(k), move(v)));
			},
		on(update_topic, atom("erase"), arg_match) >> [=](key k)
			{
			datastore->erase(k);

			if ( ! clones.empty() )
				publish(make_message(atom("erase"), datastore->sequence(),
				                     move(k)));
			},
		on(update_topic, atom("clear"), arg_match) >> [=]
			{
			datastore->clear();

			if (! clones.empty() )
				publish(make_message(atom("clear"), datastore->sequence()));
			}
		};

		serving = requests.or_else(updates).or_else(
		on(atom("quit")) >> [=]
			{
			quit();
			},
		on_arg_match >> [=](down_msg d)
			{
			demonitor(d.source);
			clones.erase(d.source);
			}
		);
		}

private:

	void publish(const caf::message& msg)
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

	impl(const caf::actor& endpoint, std::string topic,
	     std::unique_ptr<store> s)
		: actor(caf::spawn<master_actor>(std::move(s), topic))
		{
		caf::anon_send(endpoint, caf::atom("sub"),
		               subscription{subscription_type::data_query, topic},
		               actor);
		caf::anon_send(endpoint, caf::atom("sub"),
		               subscription{subscription_type::data_update, topic},
		               actor);
		}

	~impl()
		{
		caf::anon_send(actor, caf::atom("quit"));
		}

	caf::actor actor;
};

} // namespace data
} // namespace broker

#endif // BROKER_DATA_MASTER_IMPL_HH
