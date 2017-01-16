#include "broker/status.hh"

#include "broker/detail/appliers.hh"
#include "broker/detail/memory_backend.hh"

namespace broker {
namespace detail {


memory_backend::memory_backend(backend_options opts)
  : options_{std::move(opts)} {
  // nop
}

result<void>
memory_backend::put(const data& key, data value, optional<timestamp> expiry) {
  store_[key] = {std::move(value), expiry};
  return {};
}

result<void> memory_backend::add(const data& key, const data& value,
                                   optional<timestamp> expiry) {
  auto i = store_.find(key);
  if (i == store_.end())
    return sc::no_such_key;
  auto result = visit(adder{value}, i->second.first);
  if (result)
    i->second.second = expiry;
  return result;
}

result<void> memory_backend::remove(const data& key, const data& value,
                                      optional<timestamp> expiry) {
  auto i = store_.find(key);
  if (i == store_.end())
    return sc::no_such_key;
  auto result = visit(remover{value}, i->second.first);
  if (result)
    i->second.second = expiry;
  return result;
}

result<void> memory_backend::erase(const data& key) {
  store_.erase(key);
  return {};
}

result<bool> memory_backend::expire(const data& key) {
  auto ts = now();
  auto i = store_.find(key);
  if (i == store_.end())
    return sc::no_such_key;
  if (!i->second.second || ts < i->second.second)
    return false;
  store_.erase(i);
  return true;
}

result<data> memory_backend::get(const data& key) const {
  auto i = store_.find(key);
  if (i == store_.end())
    return sc::no_such_key;
  return i->second.first;
}

result<data> memory_backend::get(const data& key, const data& value) const {
  auto i = store_.find(key);
  if (i == store_.end())
    return sc::no_such_key;
  // We do not use the default implementation because operating directly on the
  // stored data element is more efficient in case the visitation returns an
  // error.
  return visit(retriever{value}, i->second.first);
}

result<bool> memory_backend::exists(const data& key) const {
  return store_.count(key) == 1;
}

result<uint64_t> memory_backend::size() const {
  return store_.size();
}

result<snapshot> memory_backend::snapshot() const {
  broker::snapshot ss;
  for (auto& p : store_)
    ss.entries.emplace(p.first, p.second.first);
  return ss;
}

} // namespace detail
} // namespace broker
