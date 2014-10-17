#include "subscription.hh"

bool broker::subscription_registry::insert(subscriber s)
	{
	auto it = subs_by_actor.find(s.who.address());
	auto rval = (it == subs_by_actor.end());

	if ( ! rval )
		erase(s.who.address());

	for ( size_t tag = 0; tag < s.subscriptions.size(); ++tag )
		for ( const auto& p : s.subscriptions[tag] )
			{
			const std::string& topic_name = p.first;
			util::radix_tree<actor_set>& rt = subs_by_topic[tag];
			rt[topic_name].insert(s.who);
			all_topics[tag][topic_name] = true;
			}

	subs_by_actor[s.who.address()] = std::move(s);
	return rval;
	}

broker::util::optional<broker::subscriber>
broker::subscription_registry::erase(const caf::actor_addr& a)
	{
	auto it = subs_by_actor.find(a);

	if ( it == subs_by_actor.end() )
		return {};

	subscriber rval = std::move(it->second);
	subs_by_actor.erase(it);

	for ( size_t tag = 0; tag < rval.subscriptions.size(); ++tag )
		for ( const auto& p : rval.subscriptions[tag] )
			{
			const std::string& topic_name = p.first;
			auto it2 = subs_by_topic[tag].find(topic_name);

			if ( it2 == subs_by_topic[tag].end() )
				continue;

			actor_set& as = it2->second;
			as.erase(rval.who);

			if ( as.empty() )
				{
				subs_by_topic[tag].erase(topic_name);
				all_topics[tag].erase(topic_name);
				}
			}

	return std::move(rval);
	}

bool broker::subscription_registry::register_topic(topic t, caf::actor a)
	{
	subscriber& s = subs_by_actor[a.address()];

	if ( ! s.who )
		s.who = std::move(a);

	util::radix_tree<bool>& their_topics = s.subscriptions[+t.type];

	auto p = their_topics.insert({std::move(t.name), true});

	if ( ! p.second )
		// We already know the actor is interested in this topic.
		return false;

	const std::string& topic_name = p.first->first;

	all_topics[+t.type][topic_name] = true;
	subs_by_topic[+t.type][topic_name].insert(s.who);
	return true;
	}

bool broker::subscription_registry::unregister_topics(const topic_set& ts,
                                                      const caf::actor_addr a)
	{
	auto it = subs_by_actor.find(a);

	if ( it == subs_by_actor.end() )
		return false;

	subscriber& s = it->second;

	for ( size_t tag = 0; tag < ts.size(); ++tag )
		for ( const auto& p : ts[tag] )
			{
			const std::string& topic_name = p.first;
			s.subscriptions[tag].erase(topic_name);
			auto it2 = subs_by_topic[tag].find(topic_name);

			if ( it2 == subs_by_topic[tag].end() )
				continue;

			actor_set& as = it2->second;
			as.erase(s.who);

			if ( as.empty() )
				{
				all_topics[tag].erase(topic_name);
				subs_by_topic[tag].erase(topic_name);
				}
			}

	return true;
	}

std::deque<broker::util::radix_tree<broker::actor_set>::iterator>
broker::subscription_registry::match_prefix(const topic& t) const
	{
	return subs_by_topic[+t.type].prefix_of(t.name);
	}

broker::util::optional<const broker::actor_set&>
broker::subscription_registry::match_exact(const topic& t) const
	{
	auto it = subs_by_topic[+t.type].find(t.name);

	if ( it == subs_by_topic[+t.type].end() )
		return {};

	return it->second;
	}

void broker::topic_set_type_info::serialize(const void* ptr,
                                            caf::serializer* sink) const
	{
	auto topic_set_ptr = reinterpret_cast<const topic_set*>(ptr);
	sink->begin_sequence(topic_set_ptr->size());

	for ( size_t i = 0; i < topic_set_ptr->size(); ++i )
		{
		const auto& topic_strings = (*topic_set_ptr)[i];
		sink->begin_sequence(topic_strings.size());

		for ( const auto& ts : topic_strings )
			sink->write_value(ts.first);

		sink->end_sequence();
		}

	sink->end_sequence();
	}

void broker::topic_set_type_info::deserialize(void* ptr,
                                              caf::deserializer* source) const
	{
	auto topic_set_ptr = reinterpret_cast<topic_set*>(ptr);
	auto num_indices = source->begin_sequence();

	for ( size_t i = 0; i < num_indices; ++i )
		{
		auto& topic_strings = (*topic_set_ptr)[i];
		topic_strings.clear();
		auto num_topic_strings = source->begin_sequence();

		for ( size_t j = 0; j < num_topic_strings; ++j )
			topic_strings.insert({source->read<std::string>(), true});

		source->end_sequence();
		}

	source->end_sequence();
	}
