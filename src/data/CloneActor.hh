#ifndef BROKER_CLONEACTOR_HH
#define BROKER_CLONEACTOR_HH

#include "../Subscription.hh"
#include "RequestMsgs.hh"
#include "broker/data/Store.hh"
#include "broker/data/InMemoryStore.hh"

#include <caf/sb_actor.hpp>
#include <caf/actor_ostream.hpp>

namespace broker { namespace data {

static void dbg_dump(const caf::actor& a, const std::string& store_id,
                     const Store& store)
    {
	// TODO: remove or put this in a preprocessor macro
    std::string header = "===== " + store_id + " Contents =====";
    std::stringstream ss;
    ss << header << std::endl;

    for ( const auto& key : store.Keys() )
        ss << key << ": " << *store.Lookup(key) << std::endl;

    for ( size_t i = 0; i < header.size(); ++i )
        ss << "=";

    ss << std::endl;
    aout(a) << ss.str();
    }

class CloneActor : public caf::sb_actor<CloneActor> {
friend class caf::sb_actor<CloneActor>;

public:

	CloneActor(caf::actor endpoint, std::string topic)
		{
		using namespace std;
		using namespace caf;

		// TODO: expose this retry interval to user API
		auto sync_retry_interval = chrono::seconds(5);
		SnapshotRequest snap_req{{SubscriptionType::DATA_REQUEST, topic}, this};
		SubscriptionTopic update_topic{SubscriptionType::DATA_UPDATE, topic};

		message_handler requests {
		on_arg_match >> [=](LookupRequest r) -> message
			{
			auto val = store.Lookup(r.key);

			if ( ! val )
				return make_message(atom("null"));
			else
				return make_message(move(*val));
			},
		on_arg_match >> [=](HasKeyRequest r)
			{
			return store.HasKey(r.key);
			},
		on<KeysRequest>() >> [=]
			{
			return store.Keys();
			},
		on<SizeRequest>() >> [=]
			{
			return store.Size();
			}
		};

		message_handler updates {
		on(update_topic, atom("insert"), val<Key>, val<Val>) >> [=]
			{
			if ( master ) forward_to(master);
			},
		on(atom("insert"), arg_match) >> [=](SequenceNum sn, Key key, Val val)
			{
			auto next = store.GetSequenceNum().Next();

			if ( sn == next )
				store.Insert(move(key), move(val));
			else if ( sn > next )
				{
				sync_error(topic);
				get_snapshot(endpoint, snap_req, sync_retry_interval);
				}
			},
		on(update_topic, atom("erase"), val<Key>) >> [=]
			{
			if ( master ) forward_to(master);
			},
		on(atom("erase"), arg_match) >> [=](SequenceNum sn, Key key)
			{
			auto next = store.GetSequenceNum().Next();

			if ( sn == next )
				store.Erase(key);
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
		on(atom("clear"), arg_match) >> [=](SequenceNum sn)
			{
			auto next = store.GetSequenceNum().Next();

			if ( sn == next )
				store.Clear();
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

	void get_snapshot(const caf::actor& endpoint, const SnapshotRequest& req,
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
			on_arg_match >> [=](actor mstr, StoreSnapshot sss)
				{
				demonitor(master);
				master = mstr;
				monitor(master);
				store = InMemoryStore{move(sss)};
				become(active);
				}
		);
		}

	InMemoryStore store;
	caf::actor master = caf::invalid_actor;
	caf::behavior initializing;
	caf::behavior active;
	caf::behavior& init_state = initializing;
};

} // namespace data
} // namespace broker

#endif // BROKER_CLONEACTOR_HH
