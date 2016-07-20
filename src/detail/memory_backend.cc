#include "broker/error.hh"

#include "broker/detail/appliers.hh"
#include "broker/detail/memory_backend.hh"

namespace broker {
namespace detail {

// TODO: implement expiration logic

expected<void>
memory_backend::put(const data& key, data value, optional<time::point> expiry) {
  store_[key] = std::move(value);
  return {};
}

expected<void> memory_backend::erase(const data& key) {
  store_.erase(key);
  return {};
}

expected<void> memory_backend::add(const data& key, const data& value,
                                   time::point t) {
  auto i = store_.find(key);
  if (i == store_.end())
    return ec::no_such_key;
  if (!visit(adder{value}, i->second))
    return ec::unspecified; // TODO: decide error type
  return {};
}

expected<void> memory_backend::remove(const data& key, const data& value,
                                      time::point t) {
  auto i = store_.find(key);
  if (i == store_.end())
    return ec::no_such_key;
  if (!visit(remover{value}, i->second))
    return ec::unspecified; // TODO: decide error type
  return {};
}

expected<void> memory_backend::expire(const data& key, time::point expiry) {
  return ec::unspecified; // TODO: implement this function
}

expected<data> memory_backend::get(const data& key) const {
  auto i = store_.find(key);
  if (i == store_.end())
    return ec::no_such_key;
  return i->second;
}

expected<data> memory_backend::get(const data& key, const data& value) const {
  auto i = store_.find(key);
  if (i == store_.end())
    return ec::no_such_key;
  return visit(retriever{value}, i->second);
}

expected<bool> memory_backend::exists(const data& key) const {
  return store_.count(key) == 1;
}

expected<uint64_t> memory_backend::size() const {
  return store_.size();
}

expected<snapshot> memory_backend::snapshot() const {
  return broker::snapshot{store_};
}

} // namespace detail
} // namespace broker
