#ifndef BROKER_SUBSCRIPTION_HH
#define BROKER_SUBSCRIPTION_HH

#include <caf/actor.hpp>
#include <caf/detail/abstract_uniform_type_info.hpp>
#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>

#include <unordered_map>
#include <unordered_set>
#include <array>
#include <cstdint>

namespace broker {

enum class SubscriptionType : uint16_t {
	PRINT = 0,
	EVENT,
	LOG,
	DATA_REQUEST,     // Used by master data stores to handle requests.
	DATA_UPDATE,      // Used by master data stores to receive updates.
	NUM_TYPES         // Sentinel for last enum value.
};

constexpr std::underlying_type<SubscriptionType>::type
operator+(SubscriptionType val)
	{ return static_cast<std::underlying_type<SubscriptionType>::type>(val); }

using ActorMap = std::unordered_map<caf::actor_addr, caf::actor>;

struct SubscriptionTopic {
	SubscriptionType type;
	std::string topic;
};

inline bool operator==(const SubscriptionTopic& lhs,
                       const SubscriptionTopic& rhs)
    { return lhs.type == rhs.type && lhs.topic == rhs.topic; }

inline bool operator!=(const SubscriptionTopic& lhs,
                       const SubscriptionTopic& rhs)
    { return ! operator==(lhs,rhs); }

inline bool operator<(const SubscriptionTopic& lhs,
                      const SubscriptionTopic& rhs)
	{ return lhs.type < rhs.type || ( ! (rhs.type < lhs.type) &&
	                                  lhs.topic < rhs.topic ); }

inline bool operator>(const SubscriptionTopic& lhs,
                      const SubscriptionTopic& rhs)
    { return operator<(rhs,lhs); }

inline bool operator<=(const SubscriptionTopic& lhs,
                       const SubscriptionTopic& rhs)
    { return ! operator>(lhs,rhs); }

inline bool operator>=(const SubscriptionTopic& lhs,
                       const SubscriptionTopic& rhs)
    { return ! operator<(lhs,rhs); }

// TODO: SubscriptionSet/Map needs some work... e.g. trie more
//       optimal?  If not split in to pair for wildard versus exact matching?
using Subscriptions = std::array<std::unordered_set<std::string>,
                                 +SubscriptionType::NUM_TYPES>;
using SubscriptionsMap = std::array<std::unordered_map<std::string, ActorMap>,
                                    +SubscriptionType::NUM_TYPES>;

class Subscriptions_type_info
        : public caf::detail::abstract_uniform_type_info<Subscriptions> {
private:

	void serialize(const void* ptr, caf::serializer* sink) const override
		{
		auto subs_ptr = reinterpret_cast<const Subscriptions*>(ptr);
		sink->begin_sequence(subs_ptr->size());

		for ( size_t i = 0; i < subs_ptr->size(); ++i )
			{
			const auto& topic_strings = (*subs_ptr)[i];
			sink->begin_sequence(topic_strings.size());

			for ( const auto& ts : topic_strings )
				sink->write_value(ts);

			sink->end_sequence();
			}

		sink->end_sequence();
		}

    void deserialize(void* ptr, caf::deserializer* source) const override
		{
		auto subs_ptr = reinterpret_cast<Subscriptions*>(ptr);
		auto num_indices = source->begin_sequence();

		for ( size_t i = 0; i < num_indices; ++i )
			{
			auto& topic_strings = (*subs_ptr)[i];
			topic_strings.clear();
			auto num_topic_strings = source->begin_sequence();

			for ( size_t j = 0; j < num_topic_strings; ++j )
				topic_strings.insert(source->read<std::string>());

			source->end_sequence();
			}

		source->end_sequence();
		}
};

using SubscriptionSet = std::set<SubscriptionTopic>;

using Subscriber = std::pair<Subscriptions, caf::actor>;
using SubscriberMap = std::unordered_map<caf::actor_addr, Subscriber>;

class SubscriberBase {
public:

	bool AddSubscriber(Subscriber s)
		{
		Subscriptions& topic_set = s.first;
		caf::actor& a = s.second;
		auto it = subscribers.find(a.address());
		bool rval = it == subscribers.end();

		if ( ! rval )
			RemSubscriber(a.address());

		for ( size_t i = 0; i < topic_set.size(); ++i )
			for ( const auto& t : topic_set[i] )
				{
				subscriptions[i][t][a.address()] = a;
				topics[i].insert(t);
				}

		subscribers[a.address()] = std::move(s);
		return rval;
		}

	bool AddSubscription(SubscriptionTopic t, caf::actor a)
		{
		topics[+t.type].insert(t.topic);
		subscriptions[+t.type][t.topic][a.address()] = a;
		Subscriber& s = subscribers[a.address()];
		s.second = std::move(a);
		return s.first[+t.type].insert(std::move(t.topic)).second;
		}

	Subscriptions RemSubscriber(const caf::actor_addr& a)
		{
		auto it = subscribers.find(a);

		if ( it == subscribers.end() )
			return Subscriptions{};

		Subscriber& s = it->second;
		Subscriptions rval = s.first;

		for ( size_t i = 0; i < rval.size(); ++i )
			for ( const auto& t : rval[i] )
				{
				auto it2 = subscriptions[i].find(t);

				if ( it2 == subscriptions[i].end() )
					continue;

				ActorMap& am = it2->second;
				am.erase(a);

				if ( am.empty() )
					{
					topics[i].erase(it2->first);
					subscriptions[i].erase(it2);
					}
				}

		subscribers.erase(it);
		return rval;
		}

	bool RemSubscriptions(const Subscriptions& ss, const caf::actor_addr& a)
		{
		auto it = subscribers.find(a);

		if ( it == subscribers.end() )
			return false;

		Subscriber& s = it->second;

		for ( size_t i = 0; i < ss.size(); ++i )
			for ( const auto& t : ss[i] )
				{
				s.first[i].erase(t);

				auto it2 = subscriptions[i].find(t);

				if ( it2 == subscriptions[i].end() )
					continue;

				ActorMap& am = it2->second;
				am.erase(a);

				if ( am.empty() )
					{
					topics[i].erase(it2->first);
					subscriptions[i].erase(it2);
					}
				}

		return true;
		}

	const Subscriptions& Topics() const
		{
		return topics;
		}

	std::unordered_set<caf::actor> Match(const SubscriptionTopic& topic)
		{
		// TODO: wildcard topics
		std::unordered_set<caf::actor> rval;
		auto it = subscriptions[+topic.type].find(topic.topic);

		if ( it == subscriptions[+topic.type].end() )
			return rval;

		for ( const auto& aa : it->second )
			rval.insert(aa.second);

		return rval;
		}

private:

	SubscriberMap subscribers;
	SubscriptionsMap subscriptions;
	Subscriptions topics;
};

} // namespace broker

#endif // BROKER_SUBSCRIPTION_HH
