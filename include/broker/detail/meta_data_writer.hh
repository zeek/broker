#pragma once

#include <type_traits>
#include <unordered_map>

#include <caf/error.hpp>
#include <caf/fwd.hpp>
#include <caf/meta/annotation.hpp>
#include <caf/none.hpp>
#include <caf/variant.hpp>

#include "broker/data.hh"
#include "broker/entity_id.hh"
#include "broker/error.hh"

namespace broker {
namespace detail {

/// Writes meta information (type and size) of Broker ::data to a serializer.
class meta_data_writer {
public:
  using result_type = caf::error;

  static constexpr bool reads_state = true;

  static constexpr bool writes_state = false;

  meta_data_writer(caf::binary_serializer& sink);

  template <class T>
  caf::error operator()(const T&) {
    // Ignore fields such as expiry, publisher, etc.
    return caf::none;
  }

  caf::error operator()(const std::pair<data, entity_id>& x) {
    // Ignore the publisher ID in recording mode.
    return (*this)(x.first);
  }

  template <class K, class V>
  caf::error operator()(const std::pair<const K, V>& x) {
    BROKER_TRY((*this)(x.first));
    return (*this)(x.second);
  }

  caf::error operator()(const std::string& x) {
    BROKER_TRY(apply(data_tag<std::string>()));
    return apply(x.size());
  }

  caf::error operator()(const enum_value& x) {
    BROKER_TRY(apply(data_tag<enum_value>()));
    return apply(x.name.size());
  }

  caf::error operator()(const set& xs) {
    BROKER_TRY(apply(data_tag<set>()));
    return apply_container(xs);
  }

  caf::error operator()(const table& xs) {
    BROKER_TRY(apply(data_tag<table>()));
    return apply_container(xs);
  }

  caf::error operator()(const vector& xs) {
    BROKER_TRY(apply(data_tag<vector>()));
    return apply_container(xs);
  }

  caf::error operator()(const data& x) {
    return caf::visit(*this, x);
  }

  caf::binary_serializer& sink() {
    return sink_;
  }

  template <class T>
  caf::error apply_container(const T& xs) {
    BROKER_TRY(apply(xs.size()));
    for (const auto& x : xs)
      BROKER_TRY((*this)(x));
    return caf::none;
  }

  template <class T0, class T1, class... Ts>
  caf::error operator()(const T0& x0, const T1& x1, const Ts&... xs) {
    if (auto err = (*this)(x0))
      return err;
    return (*this)(x1, xs...);
  }

private:
  caf::error apply(data::type tag);

  caf::error apply(size_t container_size);

  caf::binary_serializer& sink_;
};

} // namespace detail
} // namespace broker
