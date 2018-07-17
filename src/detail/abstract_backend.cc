#include "broker/detail/appliers.hh"
#include "broker/detail/abstract_backend.hh"

namespace broker {
namespace detail {

expected<void> abstract_backend::add(const data& key, const data& value,
                                     data::type init_type,
                                     optional<timestamp> expiry) {
  auto v = get(key);
  if (!v) {
    if (v.error() != ec::no_such_key)
      return v.error();
  v = expected<data>{data::from_type(init_type)};
  }

  auto result = caf::visit(adder{value}, *v);
  if (!result)
    return result;
  return put(key, *v, expiry);
}

expected<void> abstract_backend::subtract(const data& key, const data& value,
                                          optional<timestamp> expiry) {
  auto v = get(key);
  if (!v)
    return v.error();
  auto result = caf::visit(remover{value}, *v);
  if (!result)
    return result;
  return put(key, *v, expiry);
}

expected<data> abstract_backend::get(const data& key, const data& value) const {
  auto k = get(key);
  if (!k)
    return k;
  return caf::visit(retriever{value}, *k);
}

} // namespace detail
} // namespace broker
