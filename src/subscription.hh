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

// TODO: subscription data structures needs some work... e.g. trie more optimal?

enum class subscription_type : uint16_t {
	print,
	event,
	log,
	data_query,       // Used by master data stores to handle requests.
	data_update,      // Used by master data stores to receive updates.
	num_types         // Sentinel for last enum value.
};

constexpr std::underlying_type<subscription_type>::type
operator+(subscription_type val)
	{ return static_cast<std::underlying_type<subscription_type>::type>(val); }

using actor_map = std::unordered_map<caf::actor_addr, caf::actor>;

struct subscription {
	subscription_type type;
	std::string topic;
};

inline bool operator==(const subscription& lhs, const subscription& rhs)
    { return lhs.type == rhs.type && lhs.topic == rhs.topic; }

inline bool operator!=(const subscription& lhs, const subscription& rhs)
    { return ! operator==(lhs,rhs); }

inline bool operator<(const subscription& lhs, const subscription& rhs)
	{ return lhs.type < rhs.type || ( ! (rhs.type < lhs.type) &&
	                                  lhs.topic < rhs.topic ); }

inline bool operator>(const subscription& lhs, const subscription& rhs)
    { return operator<(rhs,lhs); }

inline bool operator<=(const subscription& lhs, const subscription& rhs)
    { return ! operator>(lhs,rhs); }

inline bool operator>=(const subscription& lhs, const subscription& rhs)
    { return ! operator<(lhs,rhs); }

using subscriptions = std::array<std::unordered_set<std::string>,
                                 +subscription_type::num_types>;
using subscription_map = std::array<std::unordered_map<std::string, actor_map>,
                                    +subscription_type::num_types>;

class subscriptions_type_info
        : public caf::detail::abstract_uniform_type_info<subscriptions> {
private:

	void serialize(const void* ptr, caf::serializer* sink) const override
		{
		auto subs_ptr = reinterpret_cast<const subscriptions*>(ptr);
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
		auto subs_ptr = reinterpret_cast<subscriptions*>(ptr);
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

using subscriber = std::pair<subscriptions, caf::actor>;
using subscriber_map = std::unordered_map<caf::actor_addr, subscriber>;

class subscriber_base {
public:

	bool add_subscriber(subscriber s)
		{
		subscriptions& topic_set = s.first;
		caf::actor& a = s.second;
		auto it = subscribers.find(a.address());
		bool rval = it == subscribers.end();

		if ( ! rval )
			rem_subscriber(a.address());

		for ( size_t i = 0; i < topic_set.size(); ++i )
			for ( const auto& t : topic_set[i] )
				{
				subs[i][t][a.address()] = a;
				sub_topics[i].insert(t);
				}

		subscribers[a.address()] = std::move(s);
		return rval;
		}

	bool add_subscription(subscription t, caf::actor a)
		{
		sub_topics[+t.type].insert(t.topic);
		subs[+t.type][t.topic][a.address()] = a;
		subscriber& s = subscribers[a.address()];
		s.second = std::move(a);
		return s.first[+t.type].insert(std::move(t.topic)).second;
		}

	subscriptions rem_subscriber(const caf::actor_addr& a)
		{
		auto it = subscribers.find(a);

		if ( it == subscribers.end() )
			return subscriptions{};

		subscriber& s = it->second;
		subscriptions rval = s.first;

		for ( size_t i = 0; i < rval.size(); ++i )
			for ( const auto& t : rval[i] )
				{
				auto it2 = subs[i].find(t);

				if ( it2 == subs[i].end() )
					continue;

				actor_map& am = it2->second;
				am.erase(a);

				if ( am.empty() )
					{
					sub_topics[i].erase(it2->first);
					subs[i].erase(it2);
					}
				}

		subscribers.erase(it);
		return rval;
		}

	bool rem_subscriptions(const subscriptions& ss, const caf::actor_addr& a)
		{
		auto it = subscribers.find(a);

		if ( it == subscribers.end() )
			return false;

		subscriber& s = it->second;

		for ( size_t i = 0; i < ss.size(); ++i )
			for ( const auto& t : ss[i] )
				{
				s.first[i].erase(t);

				auto it2 = subs[i].find(t);

				if ( it2 == subs[i].end() )
					continue;

				actor_map& am = it2->second;
				am.erase(a);

				if ( am.empty() )
					{
					sub_topics[i].erase(it2->first);
					subs[i].erase(it2);
					}
				}

		return true;
		}

	const subscriptions& topics() const
		{
		return sub_topics;
		}

	std::unordered_set<caf::actor> match(const subscription& topic)
		{
		// TODO: wildcard topics
		std::unordered_set<caf::actor> rval;
		auto it = subs[+topic.type].find(topic.topic);

		if ( it == subs[+topic.type].end() )
			return rval;

		for ( const auto& aa : it->second )
			rval.insert(aa.second);

		return rval;
		}

private:

	subscriber_map subscribers;
	subscription_map subs;
	subscriptions sub_topics;
};

} // namespace broker

#endif // BROKER_SUBSCRIPTION_HH
