#ifndef BROKER_CLONEACTOR_HH
#define BROKER_CLONEACTOR_HH

#include "../Subscription.hh"
#include "RequestMsgs.hh"
#include "broker/data/Store.hh"
#include "broker/data/InMemoryStore.hh"

#include <cppa/cppa.hpp>

namespace broker { namespace data {

static void dbg_dump(const cppa::actor& a, const std::string& store_id,
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

class CloneActor : public cppa::sb_actor<CloneActor> {
friend class cppa::sb_actor<CloneActor>;

public:

	CloneActor(cppa::actor endpoint, std::string topic)
		{
		using namespace std;
		using namespace cppa;

		auto sync_retry_interval = chrono::seconds(5);
		SnapshotRequest snap_req{{SubscriptionType::DATA_REQUEST, topic}, this};
		auto any_topic = val<SubscriptionTopic>;

		partial_function requests {
		on_arg_match >> [=](LookupRequest r) -> any_tuple
			{
			auto val = store.Lookup(r.key);

			if ( ! val )
				return make_cow_tuple(atom("null"));
			else
				return make_cow_tuple(move(*val));
			},
		on_arg_match >> [=](HasKeyRequest r)
			{
			return make_cow_tuple(store.HasKey(r.key));
			},
		on<KeysRequest>() >> [=]
			{
			return make_cow_tuple(store.Keys());
			},
		on<SizeRequest>() >> [=]
			{
			return make_cow_tuple(store.Size());
			}
		};

		partial_function updates {
		on(any_topic, atom("insert"), val<Key>, val<Val>) >> [=]
			{
			if ( master ) forward_to(master);
			},
		on(atom("insert"), arg_match) >> [=](SequenceNum sn, Key key, Val val)
			{
			auto next = store.GetSequenceNum().Next();

			if ( sn == next )
				{
				store.Insert(move(key), move(val));
				dbg_dump(this, topic, store);
				}
			else if ( sn > next )
				{
				sync_error(topic);
				get_snapshot(endpoint, snap_req, sync_retry_interval);
				}
			},
		on(any_topic, atom("erase"), val<Key>) >> [=]
			{
			if ( master ) forward_to(master);
			},
		on(atom("erase"), arg_match) >> [=](SequenceNum sn, Key key)
			{
			auto next = store.GetSequenceNum().Next();

			if ( sn == next )
				{
				store.Erase(key);
				dbg_dump(this, topic, store);
				}
			else if ( sn > next )
				{
				sync_error(topic);
				get_snapshot(endpoint, snap_req, sync_retry_interval);
				}
			},
		on(any_topic, atom("clear")) >> [=]
			{
			if ( master ) forward_to(master);
			},
		on(atom("clear"), arg_match) >> [=](SequenceNum sn)
			{
			auto next = store.GetSequenceNum().Next();

			if ( sn == next )
				{
				store.Clear();
				dbg_dump(this, topic, store);
				}
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

	void get_snapshot(const cppa::actor& endpoint, const SnapshotRequest& req,
	                  std::chrono::duration<double> retry)
		{
		using namespace std;
		using namespace cppa;
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
				dbg_dump(this, req.st.topic, store);
				}
		);
		}

	InMemoryStore store;
	cppa::actor master = cppa::invalid_actor;
	cppa::behavior initializing;
	cppa::behavior active;
	cppa::behavior& init_state = initializing;
};

} // namespace data
} // namespace broker

#endif // BROKER_CLONEACTOR_HH
