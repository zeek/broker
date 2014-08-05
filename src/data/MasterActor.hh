#ifndef BROKER_MASTERACTOR_HH
#define BROKER_MASTERACTOR_HH

#include "broker/data/Store.hh"
#include "RequestMsgs.hh"
#include "../Subscription.hh"

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

class MasterActor : public caf::sb_actor<MasterActor> {
friend class caf::sb_actor<MasterActor>;

public:

	MasterActor(std::unique_ptr<Store> s, std::string topic)
		: store(std::move(s))
		{
		using namespace caf;
		using namespace std;

		SubscriptionTopic update_topic{SubscriptionType::DATA_UPDATE, topic};

		message_handler requests {
		on_arg_match >> [=](SnapshotRequest r)
			{
			if ( clones.find(r.clone.address()) == clones.end() )
				{
				monitor(r.clone);
				clones[r.clone.address()] = r.clone;
				}

			return make_message(this, store->Snapshot());
			},
		on_arg_match >> [=](LookupRequest r) -> message
			{
			auto val = store->Lookup(r.key);

			if ( ! val )
				return make_message(atom("null"));
			else
				return make_message(move(*val));
			},
		on_arg_match >> [=](HasKeyRequest r)
			{
			return store->HasKey(r.key);
			},
		on<KeysRequest>() >> [=]
			{
			return store->Keys();
			},
		on<SizeRequest>() >> [=]
			{
			return store->Size();
			}
		};

		message_handler updates {
		on(update_topic, atom("insert"), arg_match) >> [=](Key key, Val val)
			{
			store->Insert(key, val);

			if ( ! clones.empty() )
				publish(make_message(atom("insert"), store->GetSequenceNum(),
				                     move(key), move(val)));
			},
		on(update_topic, atom("erase"), arg_match) >> [=](Key key)
			{
			store->Erase(key);

			if ( ! clones.empty() )
				publish(make_message(atom("erase"), store->GetSequenceNum(),
				                     move(key)));
			},
		on(update_topic, atom("clear"), arg_match) >> [=]
			{
			store->Clear();

			if (! clones.empty() )
				publish(make_message(atom("clear"), store->GetSequenceNum()));
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

	std::unique_ptr<Store> store;
	std::unordered_map<caf::actor_addr, caf::actor> clones;
	caf::behavior serving;
	caf::behavior& init_state = serving;
};

} // namespace data
} // namespace broker

#endif // BROKER_MASTERACTOR_HH
