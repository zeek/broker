#include <set>
#include <cstdint>
#include <utility>

#include "broker/detail/appliers.hh"
#include "broker/detail/memory_backend.hh"

namespace broker {
namespace detail {


memory_backend::memory_backend(backend_options opts)
  : options_{std::move(opts)} {
  // nop
}

expected<void>
memory_backend::put(const data& key, data value, optional<timestamp> expiry) {
  store_[key] = {std::move(value), std::move(expiry)};
  return {};
}

expected<void> memory_backend::add(const data& key, const data& value,
								   data::type init_type,
                                   optional<timestamp> expiry) {
  auto i = store_.find(key);
  if (i == store_.end()) {
    if (init_type == data::type::none)
      return ec::type_clash;
    auto newv = std::make_pair(data::from_type(init_type), expiry);
    i = store_.emplace(std::move(key), std::move(newv)).first;
  }
  auto result = caf::visit(adder{value}, i->second.first);
  if (result)
    i->second.second = std::move(expiry);
  return result;
}

expected<void> memory_backend::subtract(const data& key, const data& value,
                                        optional<timestamp> expiry) {
  auto i = store_.find(key);
  if (i == store_.end())
    return ec::no_such_key;
  auto result = caf::visit(remover{value}, i->second.first);
  if (result)
    i->second.second = std::move(expiry);
  return result;
}

expected<void> memory_backend::erase(const data& key) {
  store_.erase(key);
  return {};
}

expected<void> memory_backend::clear() {
   store_.clear();
   return {};
}

expected<bool> memory_backend::expire(const data& key, timestamp ts) {
  auto i = store_.find(key);
  if (i == store_.end())
    return ec::no_such_key;
  if (!i->second.second || ts < i->second.second)
    return false;
  store_.erase(i);
  return true;
}

expected<data> memory_backend::get(const data& key) const {
  auto i = store_.find(key);
  if (i == store_.end())
    return ec::no_such_key;
  return i->second.first;
}

expected<data> memory_backend::keys() const {
  set keys;
  for ( auto i = store_.begin(); i != store_.end(); i++ )
    keys.insert(i->first);
  return expected<data>(std::move(keys));
}

expected<data> memory_backend::get(const data& key, const data& value) const {
  auto i = store_.find(key);
  if (i == store_.end())
    return ec::no_such_key;
  // We do not use the default implementation because operating directly on the
  // stored data element is more efficient in case the visitation returns an
  // error.
  return caf::visit(retriever{value}, i->second.first);
}

expected<bool> memory_backend::exists(const data& key) const {
  return store_.count(key) == 1;
}

expected<uint64_t> memory_backend::size() const {
  return store_.size();
}

expected<snapshot> memory_backend::snapshot() const {
  broker::snapshot ss;
  for (auto& p : store_)
    ss.emplace(p.first, p.second.first);
  return {std::move(ss)};
}

expected<expirables> memory_backend::expiries() const {
  expirables rval;

  for (auto& p : store_) {
    if (p.second.second)
      rval.emplace_back(expirable(p.first, *p.second.second));
  }

  return {std::move(rval)};
}

} // namespace detail
} // namespace broker
