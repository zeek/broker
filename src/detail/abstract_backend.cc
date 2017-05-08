#include "broker/detail/appliers.hh"
#include "broker/detail/abstract_backend.hh"

namespace broker {
namespace detail {

expected<void> abstract_backend::add(const data& key, const data& value,
                                     optional<timespan> expiry) {
  auto v = get(key);
  if (!v)
    return v.error();
  auto result = visit(adder{value}, *v);
  if (!result)
    return result;
  return put(key, *v, expiry);
}

expected<void> abstract_backend::subtract(const data& key, const data& value,
                                          optional<timespan> expiry) {
  auto v = get(key);
  if (!v)
    return v.error();
  auto result = visit(remover{value}, *v);
  if (!result)
    return result;
  return put(key, *v, expiry);
}

expected<data> abstract_backend::get(const data& key, const data& value) const {
  auto k = get(key);
  if (!k)
    return k;
  return visit(retriever{value}, *k);
}

} // namespace detail
} // namespace broker
