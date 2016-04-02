#include "subscription.hh"

namespace broker {

bool subscription_registry::insert(subscriber s) {
  auto it = subs_by_actor.find(s.who.address());
  auto rval = (it == subs_by_actor.end());
  if (!rval)
    erase(s.who.address());
  for (const auto& p : s.subscriptions) {
    const std::string& topic_name = p.first;
    subs_by_topic[topic_name].insert(s.who);
    all_topics[topic_name] = true;
  }
  subs_by_actor[s.who.address()] = std::move(s);
  return rval;
}

maybe<subscriber>
subscription_registry::erase(const caf::actor_addr& a) {
  auto it = subs_by_actor.find(a);
  if (it == subs_by_actor.end())
    return {};
  subscriber rval = std::move(it->second);
  subs_by_actor.erase(it);
  for (const auto& p : rval.subscriptions) {
    const std::string& topic_name = p.first;
    auto it2 = subs_by_topic.find(topic_name);
    if (it2 == subs_by_topic.end())
      continue;
    actor_set& as = it2->second;
    as.erase(rval.who);
    if (as.empty()) {
      subs_by_topic.erase(topic_name);
      all_topics.erase(topic_name);
    }
  }
  return std::move(rval);
}

bool subscription_registry::register_topic(topic t, caf::actor a) {
  subscriber& s = subs_by_actor[a.address()];
  if (!s.who)
    s.who = std::move(a);
  auto p = s.subscriptions.insert({std::move(t), true});
  if (!p.second)
    // We already know the actor is interested in this topic.
    return false;
  const std::string& topic_name = p.first->first;
  all_topics[topic_name] = true;
  subs_by_topic[topic_name].insert(s.who);
  return true;
}

bool subscription_registry::unregister_topic(const topic& t,
                                             const caf::actor_addr a) {
  auto it = subs_by_actor.find(a);
  if (it == subs_by_actor.end())
    return false;
  subscriber& s = it->second;
  s.subscriptions.erase(t);
  auto it2 = subs_by_topic.find(t);
  if (it2 == subs_by_topic.end())
    return true;
  actor_set& as = it2->second;
  as.erase(s.who);
  if (as.empty()) {
    all_topics.erase(t);
    subs_by_topic.erase(t);
  }
  return true;
}

std::deque<util::radix_tree<actor_set>::iterator>
subscription_registry::prefix_matches(const topic& t) const {
  return subs_by_topic.prefix_of(t);
}

actor_set
subscription_registry::unique_prefix_matches(const topic& t) const {
  auto matches = subs_by_topic.prefix_of(t);
  actor_set rval;
  for (const auto& m : matches)
    for (const auto& a : m->second)
      rval.insert(a);
  return rval;
}

maybe<const actor_set&>
subscription_registry::exact_match(const topic& t) const {
  auto it = subs_by_topic.find(t);
  if (it == subs_by_topic.end())
    return {};
  return it->second;
}

} // namespace broker
