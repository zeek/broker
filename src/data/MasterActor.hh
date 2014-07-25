#ifndef BROKER_MASTERACTOR_HH
#define BROKER_MASTERACTOR_HH

#include "broker/data/Store.hh"
#include "RequestMsgs.hh"
#include "../Subscription.hh"

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

class MasterActor : public cppa::sb_actor<MasterActor> {
friend class cppa::sb_actor<MasterActor>;

public:

	MasterActor(std::unique_ptr<Store> s, std::string topic)
		: store(std::move(s))
		{
		using namespace cppa;
		using namespace std;

		auto any_topic = val<SubscriptionTopic>;

		partial_function requests {
		on_arg_match >> [=](SnapshotRequest r)
			{
			if ( clones.find(r.clone.address()) == clones.end() )
				{
				monitor(r.clone);
				clones[r.clone.address()] = r.clone;
				}

			return make_cow_tuple(this, store->Snapshot());
			},
		on_arg_match >> [=](LookupRequest r) -> any_tuple
			{
			auto val = store->Lookup(r.key);

			if ( ! val )
				return make_cow_tuple(atom("null"));
			else
				return make_cow_tuple(move(*val));
			},
		on_arg_match >> [=](HasKeyRequest r)
			{
			return make_cow_tuple(store->HasKey(r.key));
			},
		on<KeysRequest>() >> [=]
			{
			return make_cow_tuple(store->Keys());
			},
		on<SizeRequest>() >> [=]
			{
			return make_cow_tuple(store->Size());
			}
		};

		partial_function updates {
		on(any_topic, atom("insert"), arg_match) >> [=](Key key, Val val)
			{
			store->Insert(key, val);
			dbg_dump(this, topic, *store);

			if ( ! clones.empty() )
				publish(make_cow_tuple(atom("insert"), store->GetSequenceNum(),
				                       move(key), move(val)));
			},
		on(any_topic, atom("erase"), arg_match) >> [=](Key key)
			{
			store->Erase(key);
			dbg_dump(this, topic, *store);

			if ( ! clones.empty() )
				publish(make_cow_tuple(atom("erase"), store->GetSequenceNum(),
				                       move(key)));
			},
		on(any_topic, atom("clear"), arg_match) >> [=]
			{
			store->Clear();
			dbg_dump(this, topic, *store);

			if (! clones.empty() )
				publish(make_cow_tuple(atom("clear"), store->GetSequenceNum()));
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

	void publish(const cppa::any_tuple& msg)
		{
		for ( const auto& c : clones ) send_tuple(c.second, msg);
		}

	std::unique_ptr<Store> store;
	std::unordered_map<cppa::actor_addr, cppa::actor> clones;
	cppa::behavior serving;
	cppa::behavior& init_state = serving;
};

} // namespace data
} // namespace broker

#endif // BROKER_MASTERACTOR_HH
