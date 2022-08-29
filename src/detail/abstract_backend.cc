#include "broker/detail/abstract_backend.hh"
#include "broker/detail/appliers.hh"

namespace broker::detail {

expected<void> abstract_backend::add(const data& key, const data& value,
                                     data::type init_type,
                                     std::optional<timestamp> expiry) {
  auto v = get(key);
  if (!v) {
    if (v.error() != ec::no_such_key) {
      return v.error();
    }
    v = expected<data>{data::from_type(init_type)};
  }
  auto result = visit(adder{value}, *v);
  if (result) {
    return put(key, *v, expiry);
  }
  return result;
}

expected<void> abstract_backend::subtract(const data& key, const data& value,
                                          std::optional<timestamp> expiry) {
  auto v = get(key);
  if (!v) {
    return v.error();
  }
  auto result = visit(remover{value}, *v);
  if (result) {
    return put(key, *v, expiry);
  }
  return result;
}

expected<data> abstract_backend::get(const data& key, const data& value) const {
  auto k = get(key);
  if (k) {
    return visit(retriever{value}, *k);
  }
  return k;
}

} // namespace broker::detail
