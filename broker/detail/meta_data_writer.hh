#ifndef BROKER_DETAIL_META_DATA_WRITER_HH
#define BROKER_DETAIL_META_DATA_WRITER_HH

#include <initializer_list>

#include <caf/error.hpp>
#include <caf/fwd.hpp>
#include <caf/none.hpp>
#include <caf/variant.hpp>

#include "broker/data.hh"

namespace broker {
namespace detail {

/// Writes meta information (type and size) of Broker ::data to a serializer.
class meta_data_writer {
public:
  meta_data_writer(caf::binary_serializer& sink);

  template <class T>
  caf::error operator()(const T&) {
    return apply(type_tag<T>());
  }

  template <class K, class V>
  caf::error operator()(const std::pair<const K, V>& x) {
    if (auto err = (*this)(x.first))
      return err;
    return (*this)(x.second);
  }

  caf::error operator()(const std::string& x) {
    return apply(type_tag<std::string>(), x.size());
  }

  caf::error operator()(const enum_value& x) {
    return apply(type_tag<enum_value>(), x.name.size());
  }

  caf::error operator()(const set& xs) {
    return apply_container(xs);
  }

  caf::error operator()(const table& xs) {
    return apply_container(xs);
  }

  caf::error operator()(const vector& xs) {
    return apply_container(xs);
  }

  caf::error operator()(const data& x) {
    return caf::visit(*this, x);
  }

private:
  caf::error apply(data::type tag);

  caf::error apply(data::type tag, size_t container_size);

  template <class T>
  caf::error apply_container(const T& xs) {
    if (auto err = apply(type_tag<T>(), xs.size()))
      return err;
    for (const auto& x : xs)
      if (auto err = (*this)(x))
        return err;
    return caf::none;
  }

  caf::binary_serializer& sink_;
};

} // namespace detail
} // namespace broker

#endif // BROKER_DETAIL_META_DATA_WRITER_HH
