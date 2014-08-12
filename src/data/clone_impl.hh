#ifndef BROKER_DATA_CLONE_IMPL_HH
#define BROKER_DATA_CLONE_IMPL_HH

#include "broker/data/clone.hh"
#include "broker/data/store.hh"
#include "broker/data/mem_store.hh"
#include "query_types.hh"
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
		snapshot_request snap_req{{subscription_type::data_query, topic}, this};
		subscription update_topic{subscription_type::data_update, topic};

		message_handler requests {
		on_arg_match >> [=](lookup_request r) -> message
			{
			auto v = datastore.lookup(r.key);

			if ( ! v )
				return make_message(atom("null"));
			else
				return make_message(move(*v));
			},
		on_arg_match >> [=](has_key_request r)
			{
			return datastore.has_key(r.key);
			},
		on<keys_request>() >> [=]
			{
			return datastore.keys();
			},
		on<size_request>() >> [=]
			{
			return datastore.size();
			}
		};

		message_handler updates {
		on(update_topic, atom("insert"), val<key>, val<value>) >> [=]
			{
			if ( master ) forward_to(master);
			},
		on(atom("insert"), arg_match) >> [=](sequence_num sn, key k, value v)
			{
			auto next = datastore.sequence().next();

			if ( sn == next )
				datastore.insert(move(k), move(v));
			else if ( sn > next )
				{
				sync_error(topic);
				get_snapshot(endpoint, snap_req, sync_retry_interval);
				}
			},
		on(update_topic, atom("erase"), val<key>) >> [=]
			{
			if ( master ) forward_to(master);
			},
		on(atom("erase"), arg_match) >> [=](sequence_num sn, key k)
			{
			auto next = datastore.sequence().next();

			if ( sn == next )
				datastore.erase(k);
			else if ( sn > next )
				{
				sync_error(topic);
				get_snapshot(endpoint, snap_req, sync_retry_interval);
				}
			},
		on(update_topic, atom("clear")) >> [=]
			{
			if ( master ) forward_to(master);
			},
		on(atom("clear"), arg_match) >> [=](sequence_num sn)
			{
			auto next = datastore.sequence().next();

			if ( sn == next )
				datastore.clear();
			else if ( sn > next )
				{
				sync_error(topic);
				get_snapshot(endpoint, snap_req, sync_retry_interval);
				}
			}
		};

		initializing = (
		after(chrono::seconds(0)) >> [=]
			{
			get_snapshot(endpoint, snap_req, sync_retry_interval);
			}
		);

		active = requests.or_else(updates).or_else(
		on(atom("sync")) >> [=]
			{
			get_snapshot(endpoint, snap_req, sync_retry_interval);
			},
		on_arg_match >> [=](down_msg d)
			{
			if ( d.source == master.address() )
				{
				demonitor(master);
				master = invalid_actor;
				get_snapshot(endpoint, snap_req, sync_retry_interval);
				}
			}
		);
		}

private:

	void sync_error(const std::string& topic)
		{
		aout(this) << "ERROR: clone '" << topic << "' out of sync" << std::endl;
		}

	void get_snapshot(const caf::actor& endpoint, const snapshot_request& req,
	                  std::chrono::duration<double> retry)
		{
		using namespace std;
		using namespace caf;
		sync_send(endpoint, req).then(
			on(atom("dne")) >> [=]
				{
				delayed_send(this, retry, atom("sync"));
				become(active);
				},
			on_arg_match >> [=](actor mstr, store_snapshot sss)
				{
				demonitor(master);
				master = mstr;
				monitor(master);
				datastore = mem_store{move(sss)};
				become(active);
				}
		);
		}

	mem_store datastore;
	caf::actor master = caf::invalid_actor;
	caf::behavior initializing;
	caf::behavior active;
	caf::behavior& init_state = initializing;
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
